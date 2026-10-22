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

    /// Whether the channel was told the handshake completed.
    private var reportedHandshake = false
    /// Whether the channel was told the connection closed.
    private var reportedClose = false
    /// Whether the close was initiated locally, through `close(...)`.
    private var closedLocally = false

    /// The largest UDP payload sent.
    private static var maximumDatagramSize: Int { 1350 }

    private init(
        core: consuming QUICConnectionCore,
        role: Role,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        logger: Logger
    ) {
        self.core = core
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
        let idle = configuration.maxIdleTimeout.components
        coreConfiguration.maxIdleTimeout = .milliseconds(idle.seconds * 1000 + idle.attoseconds / 1_000_000_000_000_000)
        coreConfiguration.maxDatagramSize = Self.maximumDatagramSize
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
        let core = QUICConnectionCore(
            clientWith: Self.makeCoreConfiguration(configuration, tlsOptions: tlsOptions),
            localConnectionID: SwiftNetwork.QUICConnectionID(sourceConnectionID),
            destinationConnectionID: SwiftNetwork.QUICConnectionID(8),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            role: .client,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            logger: logger
        )
        connection.makeStreamTable()
        connection.installAsyncContinuationHandler()
        return connection
    }

    static func server(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        originalDestinationConnectionID: QUICConnectionID,
        peerConnectionID: QUICConnectionID?,
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
        let core = QUICConnectionCore(
            serverWith: Self.makeCoreConfiguration(configuration, tlsOptions: tlsOptions),
            localConnectionID: SwiftNetwork.QUICConnectionID(sourceConnectionID),
            originalDestinationConnectionID: SwiftNetwork.QUICConnectionID(originalDestinationConnectionID),
            peerConnectionID: peerConnectionID.map { SwiftNetwork.QUICConnectionID($0) }
                ?? SwiftNetwork.QUICConnectionID(0),
            now: Self.now
        )
        let connection = StateMachineQUICConnection(
            core: core,
            role: .server,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            logger: logger
        )
        connection.makeStreamTable()
        connection.installAsyncContinuationHandler()
        return connection
    }

    private func makeStreamTable() {
        let table = QUICStreamTable<Consumer>(role: self.role, stateMachine: self)
        self.streamTable = table
    }

    private func installAsyncContinuationHandler() {
        #if canImport(SwiftTLS) && SWIFTTLS_CERTIFICATE_VERIFICATION
        let eventLoop = self.eventLoop
        let loopBound = NIOLoopBound(self, eventLoop: eventLoop)
        self.core.setAsyncContinuationHandler { result in
            eventLoop.execute {
                let connection = loopBound.value
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
                    self.channelView?.handshakeCompleted(peerMaxDatagramFrameSize: 0)
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

    private func connectionClosed(_ reason: QUICConnectionCore.CloseReason) {
        self.timer?.cancel()
        self.timer = nil
        guard !self.reportedClose else { return }
        self.reportedClose = true
        let error: (any Error)?
        switch reason {
        case .idleTimeout:
            error = nil
        case .local:
            error = nil
        case .peer(let code, let isApplication, let reason):
            error = code == 0 ? nil : QUICConnectionError(reason: reason, isApplication: isApplication, code: code)
        case .transportError(let code, let reason):
            error = QUICConnectionError(reason: reason, isApplication: false, code: code)
        }
        self.channelView?.connectionClosed(error: error)
    }

    // MARK: Timer

    private func rescheduleTimer() {
        guard let deadline = self.core.nextDeadline else {
            self.timer?.cancel()
            self.timer = nil
            self.timerDeadline = nil
            return
        }
        if let current = self.timerDeadline, current == deadline, self.timer != nil {
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

    func drainPacketsToSend(to transport: some QUICTransport) {
        // Stream operations since the last drain may have produced events.
        self.processEvents()
        self.openPendingStreams()
        let now = Self.now
        let remoteAddress = self.remoteAddress.value
        while true {
            var buffer = self.allocator.buffer(capacity: Self.maximumDatagramSize)
            let written = buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: Self.maximumDatagramSize) {
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

    /// Datagrams (RFC 9221) are not supported yet.
    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        false
    }

    func flushDatagrams() {}

    func dropChannelReferences() {
        self.channelView = nil
        self.streamTable?.outOfBandDrain = nil
        self.timer?.cancel()
        self.timer = nil
    }

    // MARK: Streams

    /// Opens a stream for a slot, or queues it until the peer allows more streams.
    func openStream(for handle: QUICStreamHandle, bidirectional: Bool) {
        if let id = self.core.openStream(bidirectional: bidirectional) {
            self.streamTable?.assignID(NIOQUICHelpers.QUICStreamID(rawValue: id), to: handle)
            self.streamTable?.markReady(handle: handle, events: .opened)
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
        self.core.read(from: streamID.rawValue, body)
    }

    func writableBytes(_ streamID: QUICStreamID) -> Int {
        self.core.writableBytes(on: streamID.rawValue)
    }

    @discardableResult
    func write(_ bytes: RawSpan, to streamID: QUICStreamID, fin: Bool) -> Int {
        let written = self.core.write(bytes, to: streamID.rawValue, fin: fin)
        return written
    }

    func finish(_ streamID: QUICStreamID) {
        self.core.finish(streamID.rawValue)
    }

    func resetStream(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.core.resetStream(streamID.rawValue, errorCode: code.rawValue)
    }

    func stopSending(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.core.stopSending(streamID.rawValue, errorCode: code.rawValue)
    }

    func hasStream(_ streamID: QUICStreamID) -> Bool {
        self.core.hasStream(streamID.rawValue)
    }
}
