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
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS

/// A QUIC connection driven directly from NIO through SwiftNetwork's `QUICConnectionCore`, without
/// the SwiftNetwork framework.
///
/// Inbound datagrams are decrypted in place in their `ByteBuffer`s, outbound datagrams are written
/// straight into fresh `ByteBuffer`s, and one scheduled task serves the connection's timer.
@available(anyAppleOS 26, *)
final class StateMachineQUICConnection<Consumer: QUICStreamConsumer & ~Copyable> {
    private var core: QUICConnectionCore
    let role: Role
    let localAddress: SocketAddress
    let remoteAddress: SendableAddress
    private let eventLoop: any EventLoop
    private let allocator: ByteBufferAllocator
    private var logger: Logger

    /// The channel, once it was created; dropped when the channel goes inactive.
    internal var channelView: QUICConnectionChannel<Consumer>.ConnectionView?

    /// The stream table the consumer works on.
    private(set) var streamTable: QUICStreamTable<Consumer>?

    /// Datagrams received in the current read loop, processed on `receivePacketsComplete`.
    private var inbound: Deque<ByteBuffer> = []
    private var inReadLoop = false

    private var timer: Scheduled<Void>?
    private var timerDeadline: NetworkClock.Instant?

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

    /// The largest UDP payload sent.
    private let maximumDatagramSize: Int

    /// The version a client sends its first Initial in to force Version Negotiation: a reserved
    /// version no server supports (RFC 9000 Section 15).
    private static var versionNegotiationGreaseVersion: UInt32 { 0x1a2a_3a4a }

    #if DEBUG
    /// Runs when the connection is released, so tests can check that nothing keeps it alive.
    var _forTesting_onDeinit: (() -> Void)?

    deinit {
        self._forTesting_onDeinit?()
    }
    #endif

    private init(
        core: consuming QUICConnectionCore,
        maximumDatagramSize: Int,
        role: Role,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        logger: Logger
    ) {
        self.core = core
        self.maximumDatagramSize = maximumDatagramSize
        self.role = role
        self.localAddress = localAddress
        self.remoteAddress = SendableAddress(remoteAddress)
        self.eventLoop = eventLoop
        self.allocator = ByteBufferAllocator()
        self.logger = logger
    }

    static func makeCoreConfiguration(
        _ configuration: QUICConfiguration,
        tlsOptions: SwiftTLSProtocol.Options
    ) -> QUICConnectionCore.Configuration {
        var coreConfiguration = QUICConnectionCore.Configuration(tlsOptions: tlsOptions)
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
        // A `max_datagram_frame_size` of zero is the same as none: no DATAGRAM frames (RFC 9221
        // Section 3). The SwiftNetwork driver sends at most 65535.
        let maxDatagramFrameSize = UInt16(clamping: configuration.maxDatagramFrameSize)
        coreConfiguration.maxDatagramFrameSize = maxDatagramFrameSize > 0 ? UInt64(maxDatagramFrameSize) : nil
        return coreConfiguration
    }

    static func client(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        serverName: String?,
        asyncVerifier: AsyncVerifier?,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        logger: Logger
    ) throws -> StateMachineQUICConnection<Consumer> {
        guard let serverName else {
            throw QUICError.tlsConfigurationIncomplete
        }
        let tlsOptions = try TLSProtocol.Options.clientOptions(
            from: configuration,
            asyncVerifier: asyncVerifier,
            serverName: serverName
        )
        var coreConfiguration = Self.makeCoreConfiguration(configuration, tlsOptions: tlsOptions)
        if configuration.forceVersionNegotiation {
            // The server answers with Version Negotiation, and the client starts over in QUIC
            // version 1 (RFC 9000 Section 6).
            coreConfiguration.clientInitialVersion = Self.versionNegotiationGreaseVersion
        }
        let core = QUICConnectionCore(
            clientWith: coreConfiguration,
            localConnectionID: SwiftNetwork.QUICConnectionID(sourceConnectionID),
            destinationConnectionID: SwiftNetwork.QUICConnectionID(8),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            maximumDatagramSize: coreConfiguration.maxDatagramSize,
            role: .client,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
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
        logger: Logger
    ) throws -> StateMachineQUICConnection<Consumer> {
        guard let serverName = configuration.serverName,
            let authConfig = configuration.authenticationConfiguration
        else {
            throw QUICError.tlsConfigurationIncomplete
        }
        let tlsOptions = try TLSProtocol.Options.serverOptions(
            from: configuration,
            authConfig: authConfig,
            authenticator: authenticator,
            serverName: serverName
        )
        var coreConfiguration = Self.makeCoreConfiguration(configuration, tlsOptions: tlsOptions)
        // The token for the handshake connection ID, which lets a client recognize the stateless
        // reset this server's handler sends once the connection is gone (RFC 9000 Section 10.3).
        // Only a server sends one. Zero-length connection IDs have no token to derive.
        if sourceConnectionID.length > 0 {
            coreConfiguration.statelessResetToken = statelessResetTokenGenerator.token(for: sourceConnectionID).token
        }
        let core = QUICConnectionCore(
            serverWith: coreConfiguration,
            localConnectionID: SwiftNetwork.QUICConnectionID(sourceConnectionID),
            originalDestinationConnectionID: SwiftNetwork.QUICConnectionID(originalDestinationConnectionID),
            peerConnectionID: peerConnectionID.map { SwiftNetwork.QUICConnectionID($0) }
                ?? SwiftNetwork.QUICConnectionID(0),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            maximumDatagramSize: coreConfiguration.maxDatagramSize,
            role: .server,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
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

    private static func deadline(for instant: NetworkClock.Instant) -> NIODeadline {
        let nanoseconds = NetworkClock.Instant.zero.duration(to: instant).nanoseconds
        return .uptimeNanoseconds(UInt64(max(0, nanoseconds)))
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
        self.inbound.append(envelope.data)
        return envelope.data.readableBytes
    }

    func receivePacketsComplete() {
        let now = Self.now
        while var datagram = self.inbound.popFirst() {
            datagram.withUnsafeMutableReadableBytes { pointer in
                var span = MutableSpan(_unsafeElements: pointer.bindMemory(to: UInt8.self))
                self.core.receive(&span, now: now)
            }
        }
        self.afterCoreUpdate()
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
            self.channelView?.drainOutbound()
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
            case .pathValidated, .pathFailed, .activePathChanged:
                // Everything arrives on and goes out to one path.
                ()
            case .earlyDataRejected, .transportParametersForResumption:
                // Stream consumer connections don't enable 0-RTT.
                ()
            case .streamClosed(let id):
                guard let table = self.streamTable,
                    let handle = table.handle(forID: NIOQUICHelpers.QUICStreamID(rawValue: id))
                else { continue }
                table.streamDisconnected(handle, error: nil)
            case .closed(let reason):
                self.connectionClosed(reason)
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

    private func connectionClosed(_ reason: QUICConnectionCore.CloseReason) {
        self.timer?.cancel()
        self.timer = nil
        self.timerDeadline = nil
        self.logger.debug("Connection closed", metadata: ["quic.close.reason": "\(reason)"])
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
        guard let deadline = self.core.nextDeadline else {
            // Nothing is due; a timer still armed fires as a no-op.
            return
        }
        // Rescheduling a NIO task is expensive. Keep an armed timer that fires no later than
        // needed: if it fires early, `timerFired` re-arms it for the real deadline.
        if let current = self.timerDeadline, self.timer != nil, current <= deadline {
            return
        }
        self.timer?.cancel()
        self.timerDeadline = deadline
        self.timer = self.eventLoop.assumeIsolated().scheduleTask(deadline: Self.deadline(for: deadline)) {
            self.timerFired()
        }
    }

    private func timerFired() {
        self.timer = nil
        self.timerDeadline = nil
        self.core.handleTimeout(now: Self.now)
        self.afterCoreUpdate()
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
        let remoteAddress = self.remoteAddress.value
        while true {
            var buffer = self.allocator.buffer(capacity: self.maximumDatagramSize)
            let written = buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: self.maximumDatagramSize) {
                pointer in
                var span = MutableSpan(_unsafeElements: pointer.bindMemory(to: UInt8.self))
                return self.core.send(into: &span, now: now) ?? 0
            }
            guard written > 0 else { break }
            transport.writeDatagram(AddressedEnvelope(remoteAddress: remoteAddress, data: buffer), promise: nil)
        }
        self.rescheduleTimer()
    }

    // MARK: Closing

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        guard !self.core.isTerminating else { return false }
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
        self.timer?.cancel()
        self.timer = nil
        #if canImport(SwiftTLS)
        // The core releases the handler only once the handshake is confirmed or the connection
        // closed. A connection closed during its handshake stops its timer here and never gets
        // there, so break the cycle through the handler now.
        self.core.setAsyncContinuationHandler(nil)
        self.asyncContinuationTarget?.connection = nil
        self.asyncContinuationTarget = nil
        #endif
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
