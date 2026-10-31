//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import DequeModule
import Logging
import NIOCore
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import QUICStateMachine
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS

/// A QUIC connection driven directly from NIO through the `QUICConnectionCore` of
/// swift-network-evolution's QUICStateMachine, without the SwiftNetwork framework.
///
/// Inbound datagrams are decrypted in place in their `ByteBuffer`s, the outbound datagrams of a
/// drain are written back to back into one `ByteBuffer` (see ``DatagramBatch``), and the timer is a
/// slot in the ``StateMachineTimerQueue`` of the connection's handler.
@available(anyAppleOS 26, *)
final class StateMachineQUICConnection<Consumer: QUICStreamConsumer & ~Copyable>: StateMachineTimerTarget {
    private var core: QUICConnectionCore
    let role: Role
    let localAddress: SocketAddress
    /// The address of the active path, which the channel reports.
    let remoteAddress: SendableAddress
    private let eventLoop: any EventLoop
    private let allocator: ByteBufferAllocator
    private var logger: Logger

    /// The channel, once it was created; dropped when the channel goes inactive.
    internal var channelView: QUICConnectionChannel<Consumer>.ConnectionView?

    /// The stream table the consumer works on.
    private(set) var streamTable: QUICStreamTable<Consumer>?

    /// Datagrams received in the current read loop, processed on `receivePacketsComplete`.
    private var inbound: Deque<AddressedEnvelope<ByteBuffer>> = []

    /// The core paths a server knows, by the client address they lead to, in the order they were
    /// added; the handshake's path first. A client only has the one to the server.
    private var paths: [(address: SocketAddress, id: CorePathID)]
    private var nextPathID = CorePathID.initial.rawValue + 1

    /// The most paths a server keeps. Anyone who sees a connection's packets can send copies from
    /// other addresses, each of which the core takes for a new path (RFC 9000 Section 9.3.2), so
    /// a new address beyond this replaces the oldest path other than the active one.
    static var maximumPaths: Int { 4 }
    private var inReadLoop = false

    /// The handler's timer queue, shared by its state machine connections, and the slot armed in
    /// it, if any. The slot holds the connection until it fires or is cancelled.
    private let timerQueue: StateMachineTimerQueue
    private var timer: StateMachineTimerQueue.Slot?

    /// A datagram the core built for a later departure, and when it leaves (RFC 9002 Section
    /// 7.7). The core paces each datagram as it builds it, so the driver holds this one, and
    /// builds no more, until the timer sends it.
    private var pacedDatagram: (datagram: AddressedEnvelope<ByteBuffer>, departure: NetworkClock.Instant)?

    /// Where the drains write the datagrams for the active path.
    private var batch: DatagramBatch

    /// Locally opened streams waiting for the peer to allow more streams.
    private var pendingOpens: Deque<(handle: QUICStreamHandle, bidirectional: Bool)> = []

    /// Whether a drain of the connection's output is scheduled for the next loop tick.
    private var drainScheduled = false

    /// Whether the channel was told the handshake completed.
    private var reportedHandshake = false
    /// Whether the channel was told the connection closed.
    private var reportedClose = false
    /// Whether the close was initiated locally, through `close(...)`.
    private var closedLocally = false
    /// Whether the core reported `.closed`: it needs no more driving.
    private(set) var isClosed = false

    /// Where the connection sends once its channel is gone, while the core closes or drains.
    private var closingTransport: QUICHandler<Consumer>.ChildView?
    /// Runs when the core closed after the channel went away; the handler stops routing to the
    /// connection. Holds the handler, which holds the connection, until then.
    private var onClosedWithoutChannel: (() -> Void)?

    /// The UDP payload size the connection starts with, and the largest it sends on paths other
    /// than the active one. PMTUD may raise it on the active path, see `makeCoreConfiguration`.
    private let baseDatagramSize: Int

    /// The version a client sends its first Initial in to force Version Negotiation: a reserved
    /// version no server supports (RFC 9000 Section 15).
    private static var versionNegotiationGreaseVersion: UInt32 { 0x1a2a_3a4a }

    #if DEBUG
    /// Runs when the connection is released, so tests can check that nothing keeps it alive.
    var _forTesting_onDeinit: (() -> Void)?

    deinit {
        self._forTesting_onDeinit?()
    }

    /// The core's congestion window, in bytes.
    var _forTesting_congestionWindow: Int {
        self.core.congestionWindow
    }

    /// The paths the driver keeps.
    var _forTesting_pathCount: Int {
        self.paths.count
    }

    var _forTesting_timerQueue: StateMachineTimerQueue {
        self.timerQueue
    }

    /// Whether the connection's timer is armed.
    var _forTesting_isTimerArmed: Bool {
        self.timer.map { self.timerQueue.isArmed($0) } ?? false
    }

    /// The largest UDP payload the connection sends on the active path now, but for PMTUD probes.
    var _forTesting_datagramSize: Int {
        self.core.maxDatagramSize
    }

    /// The envelopes the drains sent on the active path, the datagrams in them, and the envelopes
    /// with more than one datagram. Datagrams held for pacing aren't counted.
    var _forTesting_envelopesSent: Int {
        self.batch.envelopesSent
    }

    var _forTesting_datagramsSent: Int {
        self.batch.datagramsSent
    }

    var _forTesting_segmentedEnvelopesSent: Int {
        self.batch.segmentedEnvelopesSent
    }

    /// The datagrams held back for a later departure.
    private(set) var _forTesting_datagramsHeld = 0

    /// The datagram held back for a later departure, if any.
    var _forTesting_heldDatagram: ByteBuffer? {
        self.pacedDatagram?.datagram.data
    }

    /// The most datagrams the drains send in one envelope; more than one only works where NIO
    /// supports UDP GSO.
    var _forTesting_maximumSegments: Int {
        get { self.batch.maximumSegments }
        set { self.batch.maximumSegments = newValue }
    }

    /// Runs with every datagram the drains write for the active path, in order.
    var _forTesting_onDatagramWritten: ((ByteBuffer) -> Void)? {
        get { self.batch.onDatagramWritten }
        set { self.batch.onDatagramWritten = newValue }
    }
    #endif

    private init(
        core: consuming QUICConnectionCore,
        baseDatagramSize: Int,
        role: Role,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        timerQueue: StateMachineTimerQueue,
        logger: Logger
    ) {
        self.core = core
        self.baseDatagramSize = baseDatagramSize
        self.role = role
        self.localAddress = localAddress
        self.remoteAddress = SendableAddress(remoteAddress)
        self.paths = [(remoteAddress, .initial)]
        self.eventLoop = eventLoop
        self.timerQueue = timerQueue
        self.allocator = ByteBufferAllocator()
        // Runs of datagrams go to the kernel in one send where it segments them (UDP GSO).
        self.batch = DatagramBatch(allocator: self.allocator)
        self.logger = logger
    }

    /// The core configuration of a client connection to the server called `serverName`, which it
    /// verifies with `asyncVerifier` or the configuration's raw public key.
    static func clientCoreConfiguration(
        _ configuration: QUICConfiguration,
        serverName: String?,
        asyncVerifier: AsyncVerifier?,
        remoteAddress: SocketAddress
    ) throws -> QUICConnectionCore.Configuration {
        guard let serverName else {
            throw QUICError.tlsConfigurationIncomplete
        }
        let tls = try SwiftTLSOptions.clientOptions(
            from: configuration,
            asyncVerifier: asyncVerifier,
            serverName: serverName
        )
        var coreConfiguration = Self.makeCoreConfiguration(configuration, tls: tls, remoteAddress: remoteAddress)
        if configuration.forceVersionNegotiation {
            // The server answers with Version Negotiation, and the client starts over in QUIC
            // version 1 (RFC 9000 Section 6).
            coreConfiguration.clientInitialVersion = Self.versionNegotiationGreaseVersion
        }
        return coreConfiguration
    }

    /// The core configuration of a server connection whose handshake connection ID is
    /// `sourceConnectionID`, which authenticates with the certificates of `authenticator` or the
    /// configuration's raw public key.
    static func serverCoreConfiguration(
        _ configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        statelessResetTokenGenerator: any QUICStatelessResetToken.Generator,
        authenticator: Authenticator?,
        remoteAddress: SocketAddress
    ) throws -> QUICConnectionCore.Configuration {
        guard let serverName = configuration.serverName,
            let authConfig = configuration.authenticationConfiguration
        else {
            throw QUICError.tlsConfigurationIncomplete
        }
        let tls = try SwiftTLSOptions.serverOptions(
            from: configuration,
            authConfig: authConfig,
            authenticator: authenticator,
            serverName: serverName
        )
        var coreConfiguration = Self.makeCoreConfiguration(configuration, tls: tls, remoteAddress: remoteAddress)
        // The token for the handshake connection ID, which lets a client recognize the stateless
        // reset this server's handler sends once the connection is gone (RFC 9000 Section 10.3).
        // Only a server sends one. Zero-length connection IDs have no token to derive.
        if sourceConnectionID.length > 0 {
            coreConfiguration.statelessResetToken = statelessResetTokenGenerator.token(for: sourceConnectionID).token
        }
        return coreConfiguration
    }

    private static func makeCoreConfiguration(
        _ configuration: QUICConfiguration,
        tls: SwiftTLSOptions,
        remoteAddress: SocketAddress
    ) -> QUICConnectionCore.Configuration {
        // Stream consumer connections don't enable 0-RTT, so there are no transport parameters
        // to resume with.
        var coreConfiguration = QUICConnectionCore.Configuration(tls: tls, resumedTransportParameters: nil)
        coreConfiguration.initialMaxData = UInt64(configuration.initialMaxData)
        coreConfiguration.initialMaxStreamDataBidirectionalLocal = UInt64(configuration.initialMaxStreamDataBidiLocal)
        coreConfiguration.initialMaxStreamDataBidirectionalRemote = UInt64(configuration.initialMaxStreamDataBidiRemote)
        coreConfiguration.initialMaxStreamDataUnidirectional = UInt64(configuration.initialMaxStreamDataUni)
        coreConfiguration.initialMaxStreamsBidirectional = UInt64(configuration.initialMaxStreamsBidi)
        coreConfiguration.initialMaxStreamsUnidirectional = UInt64(configuration.initialMaxStreamsUni)
        coreConfiguration.maxIdleTimeout = NetworkDuration(duration: configuration.maxIdleTimeout)
        // As on the SwiftNetwork driver, keepalives never close the connection: the idle timeout
        // does once they go unanswered.
        coreConfiguration.keepAliveInterval = configuration.keepAliveInterval.map(NetworkDuration.init(duration:))
        // Initial packets are padded to the configured size, and no packet is larger. Every QUIC
        // path carries 1200 bytes (RFC 9000 Section 14).
        let packetSize = max(configuration.initialPacketSize.size(forClientInitial: nil), 1200)
        coreConfiguration.initialDatagramSize = packetSize
        coreConfiguration.maxDatagramSize = packetSize
        if let maximum = configuration.initialPacketSize.probingMaximum {
            // Search for larger packets, up to the maximum, once the handshake is confirmed (RFC
            // 9000 Section 14.3, RFC 8899). The core caps the search at the interface MTU, which
            // includes the IP and UDP headers, so no payload exceeds the maximum; paths the
            // server adds later get the same cap. The search starts at an MTU of 1280 bytes, so
            // a smaller maximum has nothing to probe for: the first probe would exceed it.
            let headerSize = Self.ipUDPHeaderSize(to: remoteAddress)
            if maximum > packetSize, maximum &+ headerSize >= Self.minimumPMTUDMTU {
                coreConfiguration.pmtudEnabled = true
                coreConfiguration.ipUDPHeaderSize = headerSize
                coreConfiguration.interfaceMTU = maximum &+ headerSize
            }
        }
        // A `max_datagram_frame_size` of zero is the same as none: no DATAGRAM frames (RFC 9221
        // Section 3). The SwiftNetwork driver sends at most 65535.
        let maxDatagramFrameSize = UInt16(clamping: configuration.maxDatagramFrameSize)
        coreConfiguration.maxDatagramFrameSize = maxDatagramFrameSize > 0 ? UInt64(maxDatagramFrameSize) : nil
        // Spread each congestion window over the RTT rather than sending it in one burst (RFC
        // 9002 Section 7.7); `drainPacketsToSend(to:)` holds datagrams for their departure time.
        // Off by default, as on the SwiftNetwork driver, which never paces for NIOQUIC.
        coreConfiguration.pacingEnabled = configuration.pacePackets
        // The driver issues no spare connection IDs and routes none (`.newLocalConnectionID`),
        // so a peer can't migrate to a new connection ID: it only follows a NAT rebinding,
        // which keeps the connection IDs (RFC 9000 Sections 9 and 18.2).
        coreConfiguration.disableActiveMigration = true
        return coreConfiguration
    }

    /// The smallest MTU the core's PMTUD searches (`CorePMTUD.minimumMTU`): a smaller interface
    /// MTU counts as this.
    private static var minimumPMTUDMTU: Int { 1280 }

    /// The size of the IP and UDP headers of a datagram to `address`: 28 bytes over IPv4, 48 over
    /// IPv6. An IPv4-mapped IPv6 address (RFC 4291 Section 2.5.5.2) goes over IPv4.
    private static func ipUDPHeaderSize(to address: SocketAddress) -> Int {
        switch address {
        case .v4:
            return 28
        case .v6(let address):
            let words = withUnsafeBytes(of: address.address.sin6_addr) {
                $0.loadUnaligned(as: (UInt32, UInt32, UInt32, UInt32).self)
            }
            if words.0 == 0, words.1 == 0, words.2 == UInt32(0x0000_ffff).bigEndian {
                return 28
            }
            return 48
        case .unixDomainSocket:
            preconditionFailure("QUIC runs over UDP")
        }
    }

    static func client(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        serverName: String?,
        asyncVerifier: AsyncVerifier?,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        timerQueue: StateMachineTimerQueue,
        logger: Logger
    ) throws -> StateMachineQUICConnection<Consumer> {
        let coreConfiguration = try Self.clientCoreConfiguration(
            configuration,
            serverName: serverName,
            asyncVerifier: asyncVerifier,
            remoteAddress: remoteAddress
        )
        let core = QUICConnectionCore(
            clientWith: coreConfiguration,
            localConnectionID: QUICStateMachine.QUICConnectionID(sourceConnectionID),
            destinationConnectionID: QUICStateMachine.QUICConnectionID(8),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            baseDatagramSize: coreConfiguration.maxDatagramSize,
            role: .client,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            timerQueue: timerQueue,
            logger: logger
        )
        connection.makeStreamTable(configuration: configuration)
        connection.installAsyncContinuationHandler()
        return connection
    }

    static func server(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        originalDestinationConnectionID: QUICConnectionID,
        peerConnectionID: QUICConnectionID?,
        statelessResetTokenGenerator: any QUICStatelessResetToken.Generator,
        authenticator: Authenticator?,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        timerQueue: StateMachineTimerQueue,
        logger: Logger
    ) throws -> StateMachineQUICConnection<Consumer> {
        let coreConfiguration = try Self.serverCoreConfiguration(
            configuration,
            sourceConnectionID: sourceConnectionID,
            statelessResetTokenGenerator: statelessResetTokenGenerator,
            authenticator: authenticator,
            remoteAddress: remoteAddress
        )
        let core = QUICConnectionCore(
            serverWith: coreConfiguration,
            localConnectionID: QUICStateMachine.QUICConnectionID(sourceConnectionID),
            originalDestinationConnectionID: QUICStateMachine.QUICConnectionID(originalDestinationConnectionID),
            peerConnectionID: peerConnectionID.map { QUICStateMachine.QUICConnectionID($0) }
                ?? QUICStateMachine.QUICConnectionID(0),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            baseDatagramSize: coreConfiguration.maxDatagramSize,
            role: .server,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            timerQueue: timerQueue,
            logger: logger
        )
        connection.makeStreamTable(configuration: configuration)
        connection.installAsyncContinuationHandler()
        return connection
    }

    private func makeStreamTable(configuration: QUICConfiguration) {
        let table = QUICStreamTable<Consumer>(role: self.role, stateMachine: self)
        // The core grows a stream's window only as the application consumes (CoreStreams'
        // `updateReceiveLimit`), so reads must not wait for more than it lets in.
        table.receiveWindows = ReceiveWindows(configuration)
        self.streamTable = table
    }

    /// What the async continuation handler resumes.
    ///
    /// SwiftTLS keeps copies of the handler, in its handshake state machines, past
    /// `setAsyncContinuationHandler(nil)`, so the handler reaches the connection through this
    /// target: `dropChannelReferences()` clears it, which breaks the cycle through the core even
    /// if the handshake never completes.
    private final class AsyncContinuationTarget {
        var connection: StateMachineQUICConnection?

        init(_ connection: StateMachineQUICConnection) {
            self.connection = connection
        }
    }

    private var asyncContinuationTarget: AsyncContinuationTarget?

    private func installAsyncContinuationHandler() {
        #if canImport(SwiftTLS)
        let eventLoop = self.eventLoop
        let target = AsyncContinuationTarget(self)
        self.asyncContinuationTarget = target
        let loopBound = NIOLoopBound(target, eventLoop: eventLoop)
        self.core.setAsyncContinuationHandler { result in
            eventLoop.execute {
                // The connection is gone once its channel was torn down.
                guard let connection = loopBound.value.connection else { return }
                connection.core.resumeHandshake(with: result, now: Self.now)
                connection.afterCoreUpdate()
            }
        }
        #endif
    }

    // MARK: Clock

    static var now: NetworkClock.Instant {
        NetworkClock.Instant.zero.advanced(by: .microseconds(Int64(NIODeadline.now().uptimeNanoseconds / 1000)))
    }

    // MARK: Driver

    func setDriver(_ channel: QUICConnectionChannel<Consumer>) {
        self.channelView = channel.connectionView
        // The cycle through `outOfBandDrain` is broken in `dropChannelReferences()`.
        self.streamTable?.outOfBandDrain = {
            self.channelView?.drainStreams()
        }
    }

    var isTerminating: Bool {
        self.core.isTerminating
    }

    @discardableResult
    func receivePacket(_ envelope: AddressedEnvelope<ByteBuffer>) -> Int {
        if !self.inReadLoop {
            self.inReadLoop = true
            self.streamTable?.inReadLoop = true
        }
        self.inbound.append(envelope)
        return envelope.data.readableBytes
    }

    func receivePacketsComplete() {
        let now = Self.now
        while var envelope = self.inbound.popFirst() {
            let path = self.path(for: envelope.remoteAddress)
            envelope.data.withUnsafeMutableReadableBytes { pointer in
                var span = MutableSpan(_unsafeElements: pointer.bindMemory(to: UInt8.self))
                self.core.receive(&span, on: path, now: now)
            }
        }
        self.afterCoreUpdate()
    }

    // MARK: Paths

    /// The core path for a datagram from `address`, added if it is new.
    ///
    /// The core moves a server to a path once a non-probing packet arrives there, and validates
    /// the path (RFC 9000 Section 9.3), which is how a server follows a client's NAT rebinding.
    private func path(for address: SocketAddress) -> CorePathID {
        // A client only talks to the server's address; the server doesn't migrate.
        guard self.role == .server else { return self.core.activePathID }
        if let path = self.paths.first(where: { $0.address == address }) {
            return path.id
        }
        if self.paths.count >= Self.maximumPaths {
            let activePath = self.core.activePathID
            // '!' is fine: there are several paths, and only one is active.
            let index = self.paths.firstIndex { $0.id != activePath }!
            self.core.forgetPath(self.paths[index].id)
            self.paths.remove(at: index)
        }
        let path = CorePathID(self.nextPathID)
        self.nextPathID += 1
        self.paths.append((address, path))
        return path
    }

    private func address(of path: CorePathID) -> SocketAddress? {
        self.paths.first { $0.id == path }?.address
    }

    func exitReadLoop() {
        self.inReadLoop = false
        self.streamTable?.inReadLoop = false
    }

    /// Delivers the core's events and re-arms the timer.
    private func afterCoreUpdate() {
        self.processEvents()
        self.openPendingStreams()
        self.rescheduleTimer()
        if !self.inReadLoop {
            if let channelView = self.channelView {
                channelView.drainOutbound()
            } else if let transport = self.closingTransport {
                self.drainPacketsToSend(to: transport)
                transport.flush()
            }
        }
    }

    private func processEvents() {
        while let event = self.core.nextEvent() {
            switch event {
            case .connected:
                if !self.reportedHandshake {
                    self.reportedHandshake = true
                    // As the SwiftNetwork driver does, hand over the peer's advertised frame size;
                    // the core refuses a payload that doesn't fit a packet.
                    self.channelView?.handshakeCompleted(
                        peerMaxDatagramFrameSize: self.core.peerMaxDatagramFrameSize
                    )
                }
            case .streamOpened(let id):
                guard let table = self.streamTable else { continue }
                let streamID = NIOQUICHelpers.QUICStreamID(rawValue: id)
                let handle = table.insertSlot(id: streamID, state: nil)
                table.assignID(streamID, to: handle)
                table.markReady(handle: handle, events: .opened)
            case .streamReadable(let id):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.markReady(handle: handle, events: .readable)
            case .streamWritable(let id):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.markReady(handle: handle, events: .writable)
            case .streamReset(let id, let errorCode):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.peerResetStream(handle, code: QUICApplicationErrorCode(errorCode)!)
            case .stopSending(let id, let errorCode):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.peerStoppedSending(handle, code: QUICApplicationErrorCode(errorCode)!)
            case .streamFinishedSending:
                ()
            case .datagramsReadable:
                self.deliverDatagrams()
            case .newLocalConnectionID, .retiredLocalConnectionID, .newPeerConnectionID, .retiredPeerConnectionID:
                // The core issues no connection IDs on its own, and the peer's aren't routed yet.
                ()
            case .pathValidated:
                ()
            case .pathFailed(let path):
                // The core went back to a validated path if this one was active (RFC 9000 Section
                // 9.3.2); forget it unless it still is.
                if path != self.core.activePathID {
                    self.core.forgetPath(path)
                    self.paths.removeAll { $0.id == path }
                }
            case .activePathChanged(let path):
                if let address = self.address(of: path) {
                    self.logger.debug("Peer moved", metadata: [LoggingKeys.addressRemote: "\(address)"])
                    self.remoteAddress.value = address
                }
            case .earlyDataRejected, .transportParametersForResumption:
                // Stream consumer connections don't enable 0-RTT.
                ()
            case .streamClosed(let id):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.streamDisconnected(handle, error: nil)
            case .closing(let reason):
                // The connection carries no more data: tell the channel and its streams now,
                // not after the closing or draining period (RFC 9000 Section 10.2). The core
                // keeps running on this connection's timer until it reports `.closed`.
                self.logger.debug("Connection closing", metadata: ["quic.close.reason": "\(reason)"])
                // The connection sends nothing but CONNECTION_CLOSE from now on.
                self.pacedDatagram = nil
                self.reportClose(reason)
            case .closed(let reason):
                self.coreClosed(reason)
            }
        }
    }

    /// Hands the DATAGRAM payloads received (RFC 9221) to the channel.
    private func deliverDatagrams() {
        let allocator = self.allocator
        while let datagram = self.core.readDatagram({ bytes in
            var buffer = allocator.buffer(capacity: bytes.byteCount)
            _ = bytes.withUnsafeBytes { buffer.writeBytes($0) }
            return buffer
        }) {
            self.channelView?.datagramRead(datagram)
        }
    }

    /// The core is closed: nothing is left to drive, and the connection can be released.
    private func coreClosed(_ reason: QUICConnectionCore.CloseReason) {
        self.cancelTimer()
        self.isClosed = true
        self.logger.debug("Connection closed", metadata: ["quic.close.reason": "\(reason)"])
        // `.closing` always comes first, so this only reports a close the channel missed.
        self.reportClose(reason)
        self.closingTransport = nil
        if let onClosedWithoutChannel = self.onClosedWithoutChannel {
            self.onClosedWithoutChannel = nil
            onClosedWithoutChannel()
        }
    }

    /// Tells the channel, once, that the connection closed, which fails its streams with the
    /// reason.
    private func reportClose(_ reason: QUICConnectionCore.CloseReason) {
        guard !self.reportedClose else { return }
        self.reportedClose = true
        let error: (any Error)?
        switch reason {
        case .idleTimeout, .keepAliveTimeout:
            error = nil
        case .local:
            error = nil
        case .peer(let code, let isApplication, let reason):
            error = code == 0 ? nil : QUICConnectionError(reason: reason, isApplication: isApplication, code: code)
        case .transportError(let code, let reason):
            error = QUICConnectionError(reason: reason, isApplication: false, code: code)
        case .statelessReset:
            error = QUICConnectionError(reason: "stateless reset", isApplication: false, code: 0)
        }
        self.channelView?.connectionClosed(error: error)
    }

    // MARK: Timer

    private func rescheduleTimer() {
        // The core's deadlines don't include pacing: the paced datagram's departure is the
        // driver's own.
        let deadline: NetworkClock.Instant
        switch (self.core.nextDeadline, self.pacedDatagram?.departure) {
        case (let coreDeadline?, let departure?):
            deadline = min(coreDeadline, departure)
        case (let coreDeadline?, nil):
            deadline = coreDeadline
        case (nil, let departure?):
            deadline = departure
        case (nil, nil):
            // Nothing is due; a timer still armed fires as a no-op.
            return
        }
        // The queue keeps an armed timer that fires no later than needed: if it fires early,
        // `timerFired` re-arms it for the real deadline.
        self.timer = self.timerQueue.schedule(self.timer, at: deadline, target: self)
    }

    func timerFired() {
        self.timer = nil
        self.core.handleTimeout(now: Self.now)
        self.afterCoreUpdate()
    }

    /// Disarms the timer, which releases the connection from the timer queue.
    private func cancelTimer() {
        guard let timer = self.timer else { return }
        self.timer = nil
        self.timerQueue.cancel(timer)
    }

    // MARK: Output

    /// Makes sure output produced outside a read loop, such as a stream write from application code,
    /// is sent: the read loop drains on its own when it ends.
    private func outputProduced() {
        guard !self.inReadLoop, !self.drainScheduled, let channelView = self.channelView else { return }
        self.drainScheduled = true
        channelView.drainOutbound()
    }

    func drainPacketsToSend(to transport: some QUICTransport) {
        self.drainScheduled = false
        // Stream operations since the last drain may have produced events.
        self.processEvents()
        self.openPendingStreams()
        let now = Self.now
        // `now` has microseconds; a departure within that is due, so a timer fired for it
        // never finds it early.
        let dueBy = now.advanced(by: .microseconds(1))
        // PATH_CHALLENGE and PATH_RESPONSE for other paths go out first, unpaced (RFC 9000
        // Section 8.2).
        while let path = self.core.pathWithProbeToSend, let address = self.address(of: path) {
            var buffer = self.allocator.buffer(capacity: self.baseDatagramSize)
            let written = buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: self.baseDatagramSize) {
                pointer in
                var span = MutableSpan(_unsafeElements: pointer.bindMemory(to: UInt8.self))
                return self.core.send(into: &span, on: path, now: now) ?? 0
            }
            guard written > 0 else { break }
            transport.writeDatagram(AddressedEnvelope(remoteAddress: address, data: buffer), promise: nil)
        }
        if let (datagram, departure) = self.pacedDatagram {
            guard departure < dueBy else {
                // Still early: the timer drains again at its departure.
                self.rescheduleTimer()
                return
            }
            self.pacedDatagram = nil
            transport.writeDatagram(datagram, promise: nil)
        }
        let remoteAddress = self.remoteAddress.value
        while true {
            // The core may write a PMTUD probe larger than the path MTU (RFC 9000 Section 14.4).
            let written = self.batch.writeDatagram(
                capacity: self.core.datagramCapacity,
                sendingFullBufferTo: transport,
                remoteAddress: remoteAddress
            ) { span in
                self.core.send(into: &span, now: now) ?? 0
            }
            guard written > 0 else { break }
            if let departure = self.core.departureTime, departure >= dueBy {
                // The pacer holds this datagram back: keep it for the timer and stop here, as
                // every later datagram leaves later still. '!' is fine: the batch has the
                // datagram just written.
                let datagram = self.batch.finishDrain(
                    sendingTo: transport,
                    remoteAddress: remoteAddress,
                    holdingLast: true
                )!
                self.pacedDatagram = (AddressedEnvelope(remoteAddress: remoteAddress, data: datagram), departure)
                #if DEBUG
                self._forTesting_datagramsHeld += 1
                #endif
                self.rescheduleTimer()
                return
            }
        }
        _ = self.batch.finishDrain(sendingTo: transport, remoteAddress: remoteAddress)
        self.rescheduleTimer()
    }

    // MARK: Closing

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        guard !self.core.isTerminating else {
            // The connection is already closing on its own: make sure the channel learned why,
            // rather than pretending it is still alive.
            self.processEvents()
            return false
        }
        self.logger.debug(
            "Closing connection",
            metadata: [
                "quic.close.errorCode": "\(errorCode)", "quic.close.isApplication": "\(isApplicationClose)",
                "quic.close.reason": "\(reason)",
            ]
        )
        self.closedLocally = true
        self.core.close(errorCode: UInt64(errorCode), isApplication: isApplicationClose, reason: reason, now: Self.now)
        self.rescheduleTimer()
        // As the SwiftNetwork connection does, report the close right away: the channel drains the
        // CONNECTION_CLOSE and fires inactive once this returns. The core keeps answering the peer
        // for the closing period on its own timer.
        if !self.reportedClose {
            self.reportedClose = true
            self.channelView?.connectionClosed(error: nil)
        }
        return true
    }

    /// Streams run through the stream table, which closes them; there are no stream channels.
    func closeAllStreams() -> [EventLoopFuture<Void>] {
        []
    }

    /// The consumer is told about the connection closing through its stream table.
    func quiesceStreams() {}

    /// Queues a DATAGRAM frame (RFC 9221). Returns `false` if the core refused it: it doesn't fit a
    /// packet, or the connection is closing.
    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        datagram.withUnsafeReadableBytes { pointer in
            self.core.sendDatagram(RawSpan(_unsafeBytes: pointer))
        }
    }

    /// The channel drains the connection once the flush returns, which sends the queued datagrams.
    func flushDatagrams() {}

    func dropChannelReferences() {
        self.channelView = nil
        self.streamTable?.outOfBandDrain = nil
        if self.core.isTerminating && !self.isClosed {
            // The core still closes or drains (RFC 9000 Section 10.2): keep its timer, which
            // holds the connection until the core reports `.closed`. The handler may route the
            // peer's packets here meanwhile, see `continueClosing(transport:onClosed:)`.
        } else {
            self.cancelTimer()
        }
        #if canImport(SwiftTLS)
        // The core releases the handler only once the handshake is confirmed or the connection
        // closed. A connection closed during its handshake stops its timer here and never gets
        // there, so break the cycle through the handler now.
        self.core.setAsyncContinuationHandler(nil)
        self.asyncContinuationTarget?.connection = nil
        self.asyncContinuationTarget = nil
        #endif
    }

    /// Whether the channel is gone but the core still closes or drains, and wants the peer's
    /// packets until it is closed.
    var isClosingWithoutChannel: Bool {
        self.channelView == nil && self.core.isTerminating && !self.isClosed
    }

    /// Keeps driving a connection whose channel went away until its core is closed: it answers
    /// the packets `receiveWhileClosing(_:)` hands it, sending through `transport`, and runs
    /// `onClosed` when the core is closed (or not at all if `abandonClosing()` runs first).
    func continueClosing(transport: QUICHandler<Consumer>.ChildView, onClosed: @escaping () -> Void) {
        precondition(self.isClosingWithoutChannel)
        self.closingTransport = transport
        self.onClosedWithoutChannel = onClosed
    }

    /// Processes a datagram for a connection whose channel went away: while closing, the core
    /// answers with CONNECTION\_CLOSE (RFC 9000 Section 10.2.1); while draining it ignores it.
    func receiveWhileClosing(_ envelope: AddressedEnvelope<ByteBuffer>) {
        guard self.closingTransport != nil else { return }
        var datagram = envelope.data
        let now = Self.now
        datagram.withUnsafeMutableReadableBytes { pointer in
            var span = MutableSpan(_unsafeElements: pointer.bindMemory(to: UInt8.self))
            self.core.receive(&span, now: now)
        }
        self.afterCoreUpdate()
    }

    /// Stops driving a connection whose channel went away: the handler is going away.
    func abandonClosing() {
        self.cancelTimer()
        self.closingTransport = nil
        self.onClosedWithoutChannel = nil
    }

    // MARK: Streams

    /// Opens a stream for a slot, or queues it until the peer allows more streams.
    func openStream(for handle: QUICStreamHandle, bidirectional: Bool) {
        if let id = self.core.openStream(bidirectional: bidirectional) {
            self.streamTable?.assignID(NIOQUICHelpers.QUICStreamID(rawValue: id), to: handle)
            self.streamTable?.markReady(handle: handle, events: .opened)
            self.outputProduced()
        } else {
            self.pendingOpens.append((handle, bidirectional))
        }
    }

    /// Forgets a locally opened stream that never got an ID.
    func cancelPendingOpen(_ handle: QUICStreamHandle) {
        self.pendingOpens.removeAll { $0.handle == handle }
    }

    private func openPendingStreams() {
        while let (handle, bidirectional) = self.pendingOpens.first {
            guard let id = self.core.openStream(bidirectional: bidirectional) else { return }
            self.pendingOpens.removeFirst()
            self.streamTable?.assignID(NIOQUICHelpers.QUICStreamID(rawValue: id), to: handle)
            self.streamTable?.markReady(handle: handle, events: .opened)
        }
    }

    func read<Result>(
        _ streamID: QUICStreamID,
        _ body: (RawSpan, _ isFinished: Bool) -> (consumed: Int, result: Result)
    ) -> Result? {
        let result = self.core.read(from: streamID.rawValue, body)
        self.outputProduced()
        return result
    }

    func writableBytes(_ streamID: QUICStreamID) -> Int {
        self.core.writableBytes(on: streamID.rawValue)
    }

    @discardableResult
    func write(_ bytes: RawSpan, to streamID: QUICStreamID, fin: Bool) -> Int {
        let written = self.core.write(bytes, to: streamID.rawValue, fin: fin)
        self.outputProduced()
        return written
    }

    func finish(_ streamID: QUICStreamID) {
        self.core.finish(streamID.rawValue)
        self.outputProduced()
    }

    func resetStream(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.core.resetStream(streamID.rawValue, errorCode: code.rawValue)
        self.outputProduced()
    }

    func stopSending(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.core.stopSending(streamID.rawValue, errorCode: code.rawValue)
        self.outputProduced()
    }

    func hasStream(_ streamID: QUICStreamID) -> Bool {
        self.core.hasStream(streamID.rawValue)
    }
}
