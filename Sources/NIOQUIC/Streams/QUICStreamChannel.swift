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

import NIOCore
import NIOQUICHelpers
import Synchronization

/// The `Channel` of a QUIC stream on a stream table, as ``QUICStreamChannels`` makes them.
///
/// It behaves as the `SwiftNetwork` driver's stream channels do:
///
/// - Reads are on demand: bytes are read from the stream only while a `read()` is pending, and
///   stay in the connection otherwise, so flow control holds the peer back (RFC 9000 Section 4).
///   With `autoRead`, on by default, the channel reads once its initializer is done, and again
///   after each `channelReadComplete`.
/// - Writes are queued on the stream and sent at `flush()`. The channel is always writable: there
///   is no backpressure from the peer's flow control limits yet.
/// - Closing the output sends a FIN; closing the input sends STOP\_SENDING with code 0 and fires
///   `ChannelEvent.inputClosed`; closing both does both, then closes the channel. A
///   ``QUICStopSendingEvent`` or ``QUICResetStreamEvent`` sends STOP\_SENDING or RESET\_STREAM
///   with its code.
/// - The peer's RESET\_STREAM fires a ``QUICStreamResetError`` after the bytes read before it.
///   The peer's STOP\_SENDING closes the channel with a ``QUICStopSendingError``, unless
///   ``ChannelOptions/Types/HalfCloseOnStopSendingChannelOption`` is set, which fires a
///   ``QUICStopSendingEvent`` instead and keeps the input open.
/// - When the connection closes, the channel closes with its error.
///
/// The drains of the stream table only record what happened to the stream (see
/// ``record(events:id:resetCode:stopSendingCode:closeError:)``); the pipeline sees it once the
/// drain has finished, see ``QUICStreamChannelGroup/deliver()``. Pipeline operations act on the
/// stream through the table directly.
@available(anyAppleOS 26, *)
final class QUICStreamChannel: Sendable {
    let eventLoop: any EventLoop

    /// The connection channel.
    let parent: (any Channel)?

    let allocator: ByteBufferAllocator

    private let _localAddress: SocketAddress?

    /// The address of the remote peer, which follows the connection's active path.
    private let _remoteAddress: SendableAddress

    /// Whether the pipeline saw `channelActive` and not yet `channelInactive`.
    private let _isActive: Atomic<Bool>

    private let _closePromise: EventLoopPromise<Void>

    /// The pipeline, for access from any thread. On the event loop, ``State/pipeline`` has it.
    private let _pipeline: Mutex<ChannelPipeline?>

    /// Everything that changes, on the event loop.
    private let _state: NIOLoopBound<State>

    /// How a channel closed by its stream table closes.
    enum TableClose {
        /// The stream is done, or the connection closed: with the error, if any.
        case closed((any Error)?)
    }

    /// The channel's state, which only its event loop touches.
    final class State {
        let group: QUICStreamChannelGroup
        let isInbound: Bool

        /// The stream's slot in the table, until the channel closes.
        var handle: QUICStreamHandle?
        /// The stream's ID, once the stack assigned it.
        var streamID: QUICStreamID?

        var pipeline: ChannelPipeline!
        var pipelineStateMachine: QUICStreamPipelineStateMachine

        /// For an outbound stream, what sets it up, and who gets it once that's done.
        var outbound: (initializer: (any Channel, QUICStreamID) -> EventLoopFuture<Void>, promise: EventLoopPromise<any Channel>)?

        var autoRead: Bool
        var halfCloseOnStopSending: Bool

        /// Whether a `read()` waits for bytes.
        var pendingRead: Bool
        /// Bytes read from the stream, not yet fired as a `channelRead`.
        var bufferedReadData: ByteBuffer
        /// Whether the input is finished: the peer's FIN was read, or the input was closed.
        var inputEnded: Bool
        var inputClosedReported: Bool
        /// The code of the peer's RESET\_STREAM, until it was fired.
        var peerResetCode: QUICApplicationErrorCode?
        var resetReported: Bool

        // What the last drains recorded, see `deliverRecordedEvents()`.
        var isQueued: Bool
        var needsActivation: Bool
        var needsStreamRead: Bool
        var peerStopSendingCode: QUICApplicationErrorCode?
        var tableClose: TableClose?

        var isClosed: Bool
        /// Whether the pipeline saw the close: it sees nothing after it.
        var isPipelineClosed: Bool

        init(group: QUICStreamChannelGroup, isInbound: Bool, autoRead: Bool) {
            self.group = group
            self.isInbound = isInbound
            self.pipelineStateMachine = QUICStreamPipelineStateMachine()
            self.autoRead = autoRead
            self.halfCloseOnStopSending = false
            self.pendingRead = false
            self.bufferedReadData = ByteBuffer()
            self.inputEnded = false
            self.inputClosedReported = false
            self.resetReported = false
            self.isQueued = false
            self.needsActivation = false
            self.needsStreamRead = false
            self.isClosed = false
            self.isPipelineClosed = false
        }
    }

    private init(group: QUICStreamChannelGroup, isInbound: Bool) {
        self.eventLoop = group.eventLoop
        self.parent = group.parent
        self.allocator = group.allocator
        self._localAddress = group.localAddress
        self._remoteAddress = group.remoteAddress
        self._isActive = Atomic(false)
        self._closePromise = group.eventLoop.makePromise()
        self._pipeline = Mutex(nil)
        let state = State(group: group, isInbound: isInbound, autoRead: group.autoRead)
        self._state = NIOLoopBound(state, eventLoop: group.eventLoop)

        let pipeline = ChannelPipeline(channel: self)
        state.pipeline = pipeline
        self._pipeline.withLock { $0 = pipeline }
    }

    /// The channel of a stream the peer opened, in the slot `handle`.
    convenience init(inbound handle: QUICStreamHandle, group: QUICStreamChannelGroup) {
        self.init(group: group, isInbound: true)
        self._state.value.handle = handle
    }

    /// The channel of a stream opened locally, which `initializer` sets up once the stream has an
    /// ID, and `promise` gets then. See ``attach(handle:)``.
    convenience init(
        outboundWith initializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>,
        promise: EventLoopPromise<any Channel>,
        group: QUICStreamChannelGroup
    ) {
        self.init(group: group, isInbound: false)
        self._state.value.outbound = (initializer, promise)
    }

    /// The table opened the outbound stream in the slot `handle`.
    func attach(handle: QUICStreamHandle) {
        self._state.value.handle = handle
    }

    #if DEBUG
    /// The stream's slot in the table, until the channel closes.
    var _forTesting_handle: QUICStreamHandle? {
        self._state.value.handle
    }
    #endif
}

// MARK: - Drains

@available(anyAppleOS 26, *)
extension QUICStreamChannel {
    /// Records the events of a visit to the stream, for ``deliverRecordedEvents()``, and queues
    /// the channel for it. Runs inside a drain: fires nothing.
    func record(
        events: QUICStreamEvents,
        id: QUICStreamID?,
        resetCode: QUICApplicationErrorCode?,
        stopSendingCode: QUICApplicationErrorCode?,
        closeError: (any Error)?
    ) {
        let state = self._state.value
        if events.contains(.opened) {
            state.streamID = id
            state.needsActivation = true
        }
        if events.contains(.stopSending), let stopSendingCode {
            state.peerStopSendingCode = stopSendingCode
        }
        if events.contains(.reset), let resetCode, !state.inputEnded, !state.resetReported {
            // Bytes read before it go first. They're all there is: the connection drops what
            // wasn't read (RFC 9000 Section 3.2).
            state.peerResetCode = resetCode
            state.needsStreamRead = true
        }
        if events.contains(.closed) {
            // The table recycles the slot once the visit ends.
            state.handle = nil
            state.tableClose = .closed(closeError)
        }
        if !state.isQueued {
            state.isQueued = true
            state.group.enqueue(self)
        }
    }

    /// Whether to read from the stream in a visit with `events`: a read is pending.
    func wantsToRead(_ events: QUICStreamEvents) -> Bool {
        let state = self._state.value
        return events.contains(.readable) && !events.contains(.closed) && state.pendingRead && !state.isClosed
    }

    /// Reads what the stream has, in a visit, for ``deliverRecordedEvents()`` to fire.
    func read(from stream: inout QUICStream<QUICStreamChannels>) {
        let state = self._state.value
        Self.read(from: &stream, into: state)
        state.needsStreamRead = true
    }

    private static func read(from stream: inout QUICStream<QUICStreamChannels>, into state: State) {
        guard !state.inputEnded else { return }
        switch stream.read(into: &state.bufferedReadData) {
        case .endOfStream:
            state.inputEnded = true
        case .read, .nothingAvailable:
            ()
        }
    }

    /// Fires what the drains recorded since the last delivery: the activation and the
    /// initializer, the peer's STOP\_SENDING, the bytes read and the peer's FIN or RESET\_STREAM,
    /// then the close.
    func deliverRecordedEvents() {
        let state = self._state.value
        state.isQueued = false

        if state.needsActivation {
            state.needsActivation = false
            if state.tableClose == nil {
                self.becameActive(state)
            }
        }
        if let code = state.peerStopSendingCode.take(), !state.isClosed {
            self.peerStoppedSending(code: code, state)
        }
        if state.needsStreamRead, !state.isClosed, state.pipelineStateMachine.isInitialized {
            state.needsStreamRead = false
            self.streamRead(state)
        }
        if let tableClose = state.tableClose.take() {
            switch tableClose {
            case .closed(let error):
                self.close(error: error, promise: nil, state)
            }
        }
    }

    /// Fires `ChannelShouldQuiesceEvent`, as the connection closes gracefully.
    func quiesce() {
        let state = self._state.value
        if !state.isPipelineClosed {
            state.pipeline.fireUserInboundEventTriggered(ChannelShouldQuiesceEvent())
        }
    }
}

// MARK: - Activation and initializers

@available(anyAppleOS 26, *)
extension QUICStreamChannel {
    /// The stream has its ID: fire `channelRegistered` and `channelActive`, then run its
    /// initializer.
    private func becameActive(_ state: State) {
        guard !state.isPipelineClosed, !self.isActive else { return }
        self._isActive.store(true, ordering: .sequentiallyConsistent)
        state.pipeline.fireChannelRegistered()
        state.pipeline.fireChannelActive()

        if state.isInbound {
            if let delegate = state.group.delegate {
                delegate.initializeInboundStream(self)
            } else {
                _ = self.initializeInbound(initializer: nil)
            }
        } else {
            self.initializeOutbound(state)
        }
    }

    /// Runs the inbound initializer for this stream, driving the pipeline state machine and
    /// surfacing the stream once it is ready, as on the `SwiftNetwork` driver.
    ///
    /// - Returns: A future that completes when an asynchronous initializer finishes, or `nil`
    ///   when there was nothing to wait for. The connection holds `channelInactive` until then.
    func initializeInbound(initializer: QUICInboundStreamInitializer?) -> EventLoopFuture<Void>? {
        let state = self._state.value

        switch state.pipelineStateMachine.startInitializer(channelActive: self.isActive) {
        case .runInitializer:
            break
        case .ignore(let reason):
            switch consume reason {
            case .initializerComplete:
                self.tryToAutoRead(state)
            case .channelInactive:
                break
            case .initializerInProgress:
                break
            }
            return nil
        }

        switch initializer {
        case .multiplexer(let continuation):
            let future = continuation.initialize(channel: self).hop(to: self.eventLoop)
            future.assumeIsolated().whenComplete { result in
                switch result {
                case .success(let output):
                    switch state.pipelineStateMachine.markInitializerComplete() {
                    case .surfaceInitializedStream:
                        continuation.yield(output: output, channel: self)
                        self.initialized(state)
                    case .ignoreAlreadyComplete:
                        break
                    }
                case .failure:
                    self.close(promise: nil)
                }
            }
            return future.map { _ in () }

        case .closure(let initializer):
            let future = initializer(self).hop(to: self.eventLoop)
            future.assumeIsolated().whenComplete { result in
                switch result {
                case .success:
                    switch state.pipelineStateMachine.markInitializerComplete() {
                    case .surfaceInitializedStream:
                        self.initialized(state)
                    case .ignoreAlreadyComplete:
                        break
                    }
                case .failure:
                    self.close(promise: nil)
                }
            }
            return future

        case .none:
            switch state.pipelineStateMachine.markInitializerComplete() {
            case .surfaceInitializedStream:
                self.initialized(state)
            case .ignoreAlreadyComplete:
                break
            }
            return nil
        }
    }

    /// Runs the outbound initializer, and hands the channel to whoever opened the stream once it
    /// succeeded. A failing initializer fails the open and closes the stream.
    private func initializeOutbound(_ state: State) {
        guard let (initializer, promise) = state.outbound, let streamID = state.streamID else { return }

        switch state.pipelineStateMachine.startInitializer(channelActive: self.isActive) {
        case .runInitializer:
            initializer(self, streamID)
                .hop(to: self.eventLoop)
                .assumeIsolated()
                .whenComplete { result in
                    state.outbound = nil
                    switch result {
                    case .success:
                        switch state.pipelineStateMachine.markInitializerComplete() {
                        case .surfaceInitializedStream:
                            promise.succeed(self)
                            self.initialized(state)
                        case .ignoreAlreadyComplete:
                            promise.succeed(self)
                        }
                    case .failure(let error):
                        promise.fail(error)
                        self.close(promise: nil)
                    }
                }
        case .ignore(let reason):
            state.outbound = nil
            switch consume reason {
            case .initializerComplete:
                // A fresh channel can't have run its initializer.
                assertionFailure("outbound initializer already ran for stream \(streamID)")
                promise.succeed(self)
            case .initializerInProgress:
                assertionFailure("outbound initializer already in flight for stream \(streamID)")
                promise.succeed(self)
            case .channelInactive:
                // The initializer never ran: the stream has no pipeline, so don't hand it back.
                promise.fail(ChannelError.ioOnClosedChannel)
            }
        }
    }

    /// The initializer finished: fire what waited for it, and read.
    private func initialized(_ state: State) {
        if state.needsStreamRead, !state.isClosed {
            state.needsStreamRead = false
            self.streamRead(state)
        }
        self.tryToAutoRead(state)
    }
}

// MARK: - Reading

@available(anyAppleOS 26, *)
extension QUICStreamChannel {
    /// Fires the bytes read as a `channelRead`, then the end of the input or the peer's reset,
    /// then `channelReadComplete`, as on the `SwiftNetwork` driver.
    private func streamRead(_ state: State) {
        let hasReadData = state.bufferedReadData.readableBytes > 0
        if hasReadData {
            // The read is satisfied.
            state.pendingRead = false
            let data = state.bufferedReadData
            state.bufferedReadData = ByteBuffer()
            state.pipeline.fireChannelRead(data)
        }

        var endOfStream = false
        if state.inputEnded && !state.inputClosedReported {
            state.inputClosedReported = true
            endOfStream = true
            if !state.isPipelineClosed {
                state.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
            }
        } else if let code = state.peerResetCode.take() {
            state.resetReported = true
            endOfStream = true
            if !state.isPipelineClosed {
                state.pipeline.fireErrorCaught(QUICStreamResetError(code: code))
            }
        }

        if (hasReadData || endOfStream) && !state.isPipelineClosed {
            state.pipeline.fireChannelReadComplete()
            self.tryToAutoRead(state)
        }
    }

    /// Issues a `read()` on the pipeline if `autoRead` is enabled and no read is pending.
    private func tryToAutoRead(_ state: State) {
        if state.autoRead, !state.pendingRead, !state.isPipelineClosed {
            state.pipeline.read()
        }
    }

    /// Runs `body` on the stream, if it's still in the table.
    private func withStream<Result>(
        _ state: State,
        _ body: (inout QUICStream<QUICStreamChannels>) throws -> Result
    ) rethrows -> Result? {
        guard let handle = state.handle, let table = state.group.table else { return nil }
        return try table.withStream(handle: handle) { stream, _ in try body(&stream) }
    }
}

// MARK: - Closing

@available(anyAppleOS 26, *)
extension QUICStreamChannel {
    /// Whether the stream has a receive side: all but unidirectional ones this endpoint opened.
    private func hasReceiveSide(_ state: State) -> Bool {
        guard let id = state.streamID else { return false }
        switch (id.type, state.group.role) {
        case (.clientInitiatedUnidirectional, .client), (.serverInitiatedUnidirectional, .server):
            return false
        default:
            return true
        }
    }

    /// Whether the stream has a send side: all but unidirectional ones the peer opened.
    private func hasSendSide(_ state: State) -> Bool {
        guard let id = state.streamID else { return false }
        switch (id.type, state.group.role) {
        case (.clientInitiatedUnidirectional, .server), (.serverInitiatedUnidirectional, .client):
            return false
        default:
            return true
        }
    }

    /// Whether the stream can still send.
    private func canWrite(_ state: State) -> Bool {
        self.withStream(state) { $0.isSendOpen } ?? false
    }

    /// Whether the input can still be closed.
    private func canCloseInput(_ state: State) -> Bool {
        state.handle != nil && self.hasReceiveSide(state) && !state.inputClosedReported
    }

    /// Sends a FIN after what's queued.
    private func finishOutput(_ state: State) {
        _ = self.withStream(state) { stream in
            try? stream.flush(fin: true)
        }
    }

    /// Asks the peer to stop sending with STOP\_SENDING, unless it finished, and fires
    /// `ChannelEvent.inputClosed`.
    private func closeInput(code: QUICApplicationErrorCode, _ state: State) {
        if !state.inputEnded {
            _ = self.withStream(state) { stream in
                stream.sendStopSending(code: code)
            }
        }
        state.inputEnded = true
        state.bufferedReadData.clear()
        if !state.inputClosedReported {
            state.inputClosedReported = true
            if !state.isPipelineClosed {
                state.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
            }
        }
    }

    /// The peer sent STOP\_SENDING, which the connection answered with RESET\_STREAM (RFC 9000
    /// Section 3.5).
    private func peerStoppedSending(code: QUICApplicationErrorCode, _ state: State) {
        let inputFinished = !self.hasReceiveSide(state) || state.inputEnded
        if state.halfCloseOnStopSending && !inputFinished {
            if !state.isPipelineClosed {
                state.pipeline.fireUserInboundEventTriggered(QUICStopSendingEvent(code: code))
            }
        } else {
            self.close(error: QUICStopSendingError(code: code), promise: nil, state)
        }
    }

    /// Closes the channel, as NIO's channels do: the state first, then the promise, then
    /// `errorCaught` if there's an error, then `channelInactive` and `channelUnregistered` if it
    /// was active. The close future completes on the next loop tick.
    ///
    /// The stream leaves the table, which stops what's still open of it (see
    /// `QUICStreamTable.vacateSlot(_:)`). An outbound stream that never got its initializer run
    /// fails its open.
    private func close(error: (any Error)?, promise: EventLoopPromise<Void>?, _ state: State) {
        guard !state.isClosed else {
            promise?.succeed()
            return
        }
        state.isClosed = true

        if let handle = state.handle.take() {
            state.group.table?.vacateSlot(handle)
        }
        if let (_, openPromise) = state.outbound.take() {
            openPromise.fail(error ?? ChannelError.ioOnClosedChannel)
        }

        state.isPipelineClosed = true
        let wasActive = self._isActive.exchange(false, ordering: .sequentiallyConsistent)
        promise?.succeed()
        let pipeline = state.pipeline!
        if let error {
            pipeline.fireErrorCaught(error)
        }
        if wasActive {
            pipeline.fireChannelInactive()
            pipeline.fireChannelUnregistered()
        }

        self.eventLoop.assumeIsolated().execute {
            self.removeHandlers(pipeline: pipeline)
            self._closePromise.succeed()
        }
    }
}

// MARK: - Channel

@available(anyAppleOS 26, *)
extension QUICStreamChannel: Channel {
    var closeFuture: EventLoopFuture<Void> {
        self._closePromise.futureResult
    }

    var pipeline: ChannelPipeline {
        self._pipeline.withLock { $0! }
    }

    var localAddress: SocketAddress? {
        self._localAddress
    }

    var remoteAddress: SocketAddress? {
        self._remoteAddress.value
    }

    var isWritable: Bool {
        // Writes are queued on the stream: there is no backpressure yet.
        true
    }

    var isActive: Bool {
        self._isActive.load(ordering: .sequentiallyConsistent)
    }

    var _channelCore: any ChannelCore {
        self
    }

    struct SynchronousOptions: NIOSynchronousChannelOptions {
        let channel: QUICStreamChannel

        func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
            try self.channel.setOption0(option, value: value)
        }

        func getOption<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
            try self.channel.getOption0(option)
        }
    }

    var syncOptions: (any NIOSynchronousChannelOptions)? {
        SynchronousOptions(channel: self)
    }

    func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture { try self.setOption0(option, value: value) }
        } else {
            return self.eventLoop.submit { try self.setOption0(option, value: value) }
        }
    }

    func getOption<Option: ChannelOption>(_ option: Option) -> EventLoopFuture<Option.Value> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture { try self.getOption0(option) }
        } else {
            return self.eventLoop.submit { try self.getOption0(option) }
        }
    }

    private func getOption0<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
        let state = self._state.value
        switch option {
        case is ChannelOptions.Types.QUICStreamIDChannelOption:
            guard let id = state.streamID else { throw ChannelError.operationUnsupported }
            return id.rawValue as! Option.Value
        case is ChannelOptions.Types.HalfCloseOnStopSendingChannelOption:
            return state.halfCloseOnStopSending as! Option.Value
        case is ChannelOptions.Types.AutoReadOption:
            return state.autoRead as! Option.Value
        default:
            throw ChannelError.operationUnsupported
        }
    }

    private func setOption0<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
        let state = self._state.value
        switch option {
        case is ChannelOptions.Types.HalfCloseOnStopSendingChannelOption:
            state.halfCloseOnStopSending = value as! Bool
        case is ChannelOptions.Types.AutoReadOption:
            state.autoRead = value as! Bool
        default:
            throw ChannelError.operationUnsupported
        }
    }
}

// MARK: - ChannelCore

@available(anyAppleOS 26, *)
extension QUICStreamChannel: ChannelCore {
    func localAddress0() throws -> SocketAddress {
        guard let localAddress = self._localAddress else { throw ChannelError.unknownLocalAddress }
        return localAddress
    }

    func remoteAddress0() throws -> SocketAddress {
        self._remoteAddress.value
    }

    func register0(promise: EventLoopPromise<Void>?) {
        promise?.fail(ChannelError.operationUnsupported)
    }

    func bind0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        promise?.fail(ChannelError.operationUnsupported)
    }

    func connect0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        promise?.fail(ChannelError.operationUnsupported)
    }

    func write0(_ data: NIOAny, promise: EventLoopPromise<Void>?) {
        let state = self._state.value
        let buffer = self.unwrapData(data, as: ByteBuffer.self)
        // As on the `SwiftNetwork` driver, a write succeeds once it's queued.
        _ = self.withStream(state) { stream in
            stream.write(buffer)
        }
        promise?.succeed()
    }

    func flush0() {
        let state = self._state.value
        _ = self.withStream(state) { stream in
            // A send side that is gone drops the writes, as on the `SwiftNetwork` driver.
            try? stream.flush()
        }
    }

    func read0() {
        let state = self._state.value
        state.pendingRead = true
        // Satisfy the read right away if the stream has bytes.
        _ = self.withStream(state) { stream in
            Self.read(from: &stream, into: state)
        }
        self.streamRead(state)
    }

    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        let state = self._state.value
        switch mode {
        case .output:
            guard self.canWrite(state) else {
                promise?.fail(ChannelError.outputClosed)
                return
            }
            self.finishOutput(state)
            promise?.succeed()

        case .input:
            guard self.canCloseInput(state) else {
                promise?.fail(ChannelError.inputClosed)
                return
            }
            self.closeInput(code: QUICApplicationErrorCode(0)!, state)
            promise?.succeed()

        case .all:
            // Gracefully: STOP_SENDING with code 0, and a FIN after what's queued.
            if self.canCloseInput(state) {
                self.closeInput(code: QUICApplicationErrorCode(0)!, state)
            }
            if self.canWrite(state) {
                self.finishOutput(state)
            }
            self.close(error: nil, promise: promise, state)
        }
    }

    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        let state = self._state.value
        switch event {
        case let event as QUICStopSendingEvent:
            if self.canCloseInput(state) {
                self.closeInput(code: event.code, state)
            }
            if !self.hasSendSide(state) || !self.canWrite(state) {
                // Both sides are closed now.
                self.close(error: nil, promise: promise, state)
            } else {
                promise?.succeed()
            }

        case let event as QUICResetStreamEvent:
            _ = self.withStream(state) { stream in
                stream.sendResetStream(code: event.code)
            }
            promise?.succeed()

        default:
            promise?.fail(ChannelError.operationUnsupported)
        }
    }

    func channelRead0(_ data: NIOAny) {
        // Unhandled read, drop it.
    }

    func errorCaught0(error: any Error) {
        // Unhandled error, drop it.
    }
}
