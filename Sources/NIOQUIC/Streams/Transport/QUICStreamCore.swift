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

/// The transport state for a stream.
@available(anyAppleOS 26, *)
@usableFromInline
struct QUICStreamCore: ~Copyable {
    /// The ID of the stream assigned by the connection, `nil` if it hasn't been assigned yet
    /// (e.g. is locally opened).
    @usableFromInline
    private(set) var id: QUICStreamID?

    /// The state of a stream.
    private var state: QUICStreamStateMachine

    /// Set when a frame had the FIN flag set, cleared when the FIN is handed to the state machine.
    ///
    /// This is necessary so that the state machine doesn't receive the FIN before the consumer has
    /// received all inbound data (as each read must also go through the state machine).
    private var finPending: Bool

    /// Writes that haven't yet been handed to the connection, in order.
    private var pendingWrites: [ByteBuffer]

    /// Whether a read left bytes in the stream that the consumer may still want.
    @usableFromInline
    var _needsReadVisit: Bool

    /// Whether a read left bytes in the stream that the consumer may still want.
    @inlinable
    var needsReadVisit: Bool {
        self._needsReadVisit
    }

    /// Creates a stream core.
    ///
    /// - Parameter id: The stream ID, if the connection has already assigned one.
    @usableFromInline
    init(id: QUICStreamID?) {
        self.id = id
        self.state = QUICStreamStateMachine()
        self.pendingWrites = []
        self.finPending = false
        self._needsReadVisit = false
    }

    /// Whether more data can be written to the stream.
    @usableFromInline
    var isSendOpen: Bool {
        !self.state.isSendFinished
    }

    /// Whether more data can be read from the stream.
    @usableFromInline
    var isReceiveOpen: Bool {
        !self.state.isReceiveClosed
    }

    /// Records the ID the connection assigned and resolves the stream's direction.
    ///
    /// - Parameters:
    ///   - id: The stream ID the connection assigned.
    ///   - direction: The direction implied by the ID and the local role, which only the
    ///     connection knows.
    /// - Returns: The state machine's action for the transition.
    mutating func connected(
        id: QUICStreamID,
        direction: QUICStreamDirection
    ) -> QUICStreamStateMachine.StreamConnectedAction {
        self.id = id
        return self.state.streamConnected(direction: direction)
    }
}

// MARK: - Peer events

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Records the peer's RESET\_STREAM.
    ///
    /// - Parameter code: The application error code the peer sent.
    mutating func receiveResetStream(code: QUICApplicationErrorCode) {
        let action: QUICStreamStateMachine.ReceiveResetStreamAction

        do {
            // The connection owns reassembly, so there is no final size to assert here.
            action = try self.state.receiveResetStream(applicationErrorCode: code, finalSize: 0)
        } catch {
            return  // Closed, or a direction with no receive side.
        }

        switch action {
        case .closeStream:
            ()
        case .surfaceReset:
            ()
        case .doNothing(.alreadyFullyReceived):
            ()  // The peer already finished cleanly; the reset has nothing left to abandon.
        case .doNothing(.alreadyReset):
            ()  // RESET_STREAM is not acted on twice.
        }
    }
}

// MARK: - Reading

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Hands a FIN to the state machine, but only once all data has been handed over to the
    /// consumer.
    private mutating func confirmFin() {
        guard self.finPending && self.state.isConnected else {
            return
        }

        self.finPending = false

        do {
            // The connection owns reassembly, so there is no final size to assert here.
            switch try self.state.receiveFin(finalSize: 0) {
            case .markAllDataReceived:
                ()
            case .ignore(.alreadyReceivedFin):
                ()
            case .ignore(.streamReset):
                ()
            }
        } catch {
            ()  // Closed, or a direction with no receive side.
        }
    }

    /// Moves the receive side to its terminal state.
    private mutating func consumeEndOfStream() {
        do {
            // Called for the transition; what the caller is told was decided by what the consumer
            // actually took.
            switch try self.state.applicationRead() {
            case .deliverData:
                ()
            case .deliverEndOfStream:
                ()
            case .deliverResetError:
                ()
            case .ignore(.noDataAvailable):
                ()
            case .ignore(.alreadyDelivered):
                ()
            }
        } catch {
            // Not connected: a FIN which arrived before the connection assigned an ID is replayed
            // into the receive side when it does, so there is nothing to consume yet.
        }
    }
}

// MARK: - Writing

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Queues bytes filled in place by `body`.
    ///
    /// - Parameters:
    ///   - count: The number of writable bytes to hand to `body`.
    ///   - body: Handed `count` writable bytes, returning how many it wrote.
    @usableFromInline
    mutating func withWritableSpan(count: Int, _ body: (_ span: inout MutableRawSpan) -> Int) {
        var buffer = ByteBufferAllocator().buffer(capacity: count)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: count) { pointer in
            var bytes = MutableRawSpan(_unsafeBytes: UnsafeMutableRawBufferPointer(rebasing: pointer[..<count]))
            let wrote = body(&bytes)
            assert(wrote <= count, "body(_:) claimed to write more bytes than there was space for")
            assert(wrote >= 0, "body(_:) claimed to write negative bytes")
            return min(max(wrote, 0), count)
        }
        if buffer.readableBytes > 0 {
            self.pendingWrites.append(buffer)
        }
    }

    /// Queues `buffer`, without copying it.
    ///
    /// - Parameter buffer: The bytes to queue.
    @usableFromInline
    mutating func write(_ buffer: ByteBuffer) {
        if buffer.readableBytes != 0 {
            self.pendingWrites.append(buffer)
        }
    }

    /// Checks that the send side takes what is queued, and finishes it in the state machine if
    /// `fin` is set.
    ///
    /// - Returns: Whether the flush carries a FIN, or `nil` if there is nothing to flush.
    /// - Throws: ``QUICError/invalidStreamState`` if the send side can't take what was queued: it
    ///   has no ID yet, in which case the queue is kept so that the flush can be repeated once it
    ///   has; it has no send side; or FIN or RESET\_STREAM went out already. The queue is dropped
    ///   then.
    private mutating func beginFlush(fin: Bool) throws(QUICError) -> Bool? {
        if self.pendingWrites.isEmpty && !fin { return nil }

        // Refusals drop outbound writes unless the stream doesn't yet have an ID. Callers may
        // retry the flush once the stream gets an ID.
        var dropWritesOnRefusal = true
        var refused = false

        do {
            switch try self.state.writeData() {
            case .sendData:
                ()
            case .doNotWrite(.streamFinished):
                refused = true  // A FIN has already been sent.
            case .doNotWrite(.streamReset):
                refused = true  // RESET_STREAM has already been sent.
            }
        } catch {
            refused = true
            switch error {
            case .notConnected:
                // The connection hasn't assigned an ID yet, so nothing can have finished the send side,
                // refuse the flush but keep the pending writes, the flush can be retried later.
                dropWritesOnRefusal = false
            case .wrongDirection:
                ()  // The stream has no send side.
            }
        }

        var carriesFin = false

        if fin && !refused {
            do {
                switch try self.state.sendFin() {
                case .sendFin:
                    carriesFin = true
                case .ignore(.alreadyFinished):
                    refused = true
                case .ignore(.streamReset):
                    refused = true
                }
            } catch {
                assert(self.pendingWrites.isEmpty)
                throw QUICError.invalidStreamState
            }
        }

        if refused {
            if dropWritesOnRefusal {
                self.pendingWrites.removeAll()
            }
            throw QUICError.invalidStreamState
        }

        if self.pendingWrites.isEmpty && !carriesFin { return nil }
        return carriesFin
    }
}

// MARK: - Closing

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Drops whatever is still queued locally, without telling the connection.
    mutating func abandonLocally() {
        self.pendingWrites.removeAll()
    }
}

// MARK: - Operations

/// The stream operations for a stream on ``QUICStreamOperations``, as those of a
/// ``StateMachineQUICConnection``.
@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Hands the stream's readable bytes to `body` until it stops consuming them.
    ///
    /// `body` returns the number of bytes that it consumed from the span. Bytes which weren't read
    /// are held until the next read. This function will return `endOfStream` if the stream is half
    /// closed.
    ///
    /// - Parameters:
    ///   - minContiguous: The shortest run of contiguous bytes to hand to `body`; `1` hands over
    ///     any. If there aren't enough bytes available and the peer hasn't yet sent FIN then
    ///     `body` won't be called. Once the peer sent FIN, `body` may be called with fewer bytes
    ///     than `minContiguous`.
    ///   - operations: The operations that hold the stream's bytes.
    ///   - id: The stream's ID.
    ///   - body: Provided a contiguous block of bytes received from the remote peer, returning how
    ///     many of them it consumed. May be called more then once for each call to `read`.
    /// - Returns: The outcome of the read.
    mutating func read(
        minContiguous: Int,
        from operations: some QUICStreamOperations,
        id: QUICStreamID,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        self._needsReadVisit = false

        switch self.state.attemptRead() {
        case .proceedWithRead:
            break
        case .doNotRead:
            // Receive side closed: the connection holds nothing more for the consumer.
            if self.state.hasReceivedFin {
                self.consumeEndOfStream()
                return .endOfStream(0)
            }
            return .nothingAvailable
        }

        let contiguous = max(minContiguous, 1)
        var delivered = 0
        var leftOver = false
        var reachedEnd = false
        // The connection hands over all contiguous bytes at once. What `body` leaves is handed
        // over again, until `body` takes nothing more.
        while true {
            var consumed = 0
            let found: Void? = operations.read(id) { bytes, isFinished in
                let available = bytes.byteCount
                if available > 0 && (available >= contiguous || isFinished) {
                    let wanted = body(bytes)
                    assert(wanted >= 0, "body(_:) claimed to read negative bytes")
                    assert(wanted <= available, "body(_:) claimed to read more bytes than available")
                    consumed = min(max(wanted, 0), available)
                }
                leftOver = consumed < available
                reachedEnd = isFinished && !leftOver
                return (consumed, ())
            }
            delivered &+= consumed
            if found == nil || consumed == 0 || !leftOver {
                break
            }
        }

        if delivered > 0 {
            _ = try? self.state.receiveData()
        }
        if reachedEnd {
            self.finPending = true
            self.confirmFin()
            self.consumeEndOfStream()
            return .endOfStream(delivered)
        }
        if delivered > 0 {
            if leftOver {
                self._needsReadVisit = true
            }
            return .read(delivered)
        }
        return .nothingAvailable
    }

    /// How many bytes the stream will accept right now, which is advisory: a write past it is
    /// buffered rather than refused.
    ///
    /// Note that this returns `0` until the connection has assigned the stream its ID, which is not
    /// the same as no room: writes may be queued before then, but flushing them is refused until the
    /// ID arrives.
    func writableBytes(on operations: some QUICStreamOperations) -> Int {
        guard let id = self.id, !self.state.isSendFinished else { return 0 }
        return operations.writableBytes(id)
    }

    /// Hands queued writes to `operations`.
    ///
    /// - Throws: As ``beginFlush(fin:)``, or ``QUICError/streamWriteFailed`` if the connection
    ///   refused the bytes, in which case the rest of the queue is dropped.
    mutating func flush(fin: Bool, to operations: some QUICStreamOperations) throws(QUICError) {
        guard let carriesFin = try self.beginFlush(fin: fin) else { return }

        guard let id = self.id else {
            self.pendingWrites.removeAll()
            throw QUICError.invalidStreamState
        }

        // The core takes all of a write, or none once the stream or the connection can't send any
        // more: for instance while the connection is closing (RFC 9000 Section 10.2), which the
        // stream may not know yet. The rest of the queue fails with the refused write.
        var refused = false
        for buffer in self.pendingWrites {
            refused = buffer.withUnsafeReadableBytes { bytes in
                operations.write(RawSpan(_unsafeBytes: bytes), to: id, fin: false) < bytes.count
            }
            if refused { break }
        }
        self.pendingWrites.removeAll(keepingCapacity: true)
        if refused {
            throw QUICError.streamWriteFailed
        }
        if carriesFin {
            operations.finish(id)
        }
    }

    /// Resets the send side, dropping what is still queued: RESET\_STREAM abandons the stream's
    /// data.
    mutating func sendResetStream(code: QUICApplicationErrorCode, on operations: some QUICStreamOperations) {
        let action: QUICStreamStateMachine.LocalResetAction
        do {
            action = try self.state.localReset(applicationErrorCode: code)
        } catch {
            return
        }
        switch action {
        case .sendReset:
            self.pendingWrites.removeAll()
            if let id = self.id {
                operations.resetStream(id, code: code)
            }
        case .ignore(.alreadyFinished):
            ()
        case .ignore(.alreadyReset):
            ()
        }
    }

    /// Closes the receive side.
    mutating func sendStopSending(code: QUICApplicationErrorCode, on operations: some QUICStreamOperations) {
        let action: QUICStreamStateMachine.CloseReadSideAction
        do {
            action = try self.state.closeReadSide()
        } catch {
            return
        }
        switch action {
        case .markReadClosed:
            self.finPending = false
            if let id = self.id {
                operations.stopSending(id, code: code)
            }
        case .ignoreAlreadyClosed:
            ()
        case .deliverPeerResetError:
            ()
        }
    }

    /// Records the peer's STOP\_SENDING for a stream whose connection already answered it with
    /// RESET\_STREAM.
    mutating func receiveStopSendingAnsweredByStack(code: QUICApplicationErrorCode) {
        do {
            switch try self.state.receiveStopSending(applicationErrorCode: code) {
            case .sendReset:
                self.pendingWrites.removeAll()
            case .sendResetAndCloseStream:
                self.pendingWrites.removeAll()
            case .ignore:
                ()
            }
        } catch {
            ()
        }
    }

    /// Tears the stream down. Whatever direction is still open is aborted in the state machine.
    mutating func close(on operations: some QUICStreamOperations, id: QUICStreamID) {
        let sendOpen = !self.state.isSendFinished
        let receiveOpen = !self.state.isReceiveClosed
        switch self.state.close(reason: .clean) {
        case .close:
            ()
        case .ignoreAlreadyClosed:
            ()
        }
        if sendOpen {
            operations.resetStream(id, code: QUICApplicationErrorCode(0)!)
        }
        if receiveOpen {
            operations.stopSending(id, code: QUICApplicationErrorCode(0)!)
        }
        self.abandonLocally()
        self.finPending = false
    }
}

#if DEBUG
@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Whether any write is still queued for a flush.
    var _forTesting_hasPendingWrites: Bool {
        !self.pendingWrites.isEmpty
    }
}
#endif  // DEBUG
