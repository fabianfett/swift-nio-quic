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
import NIOQUICHelpers

@available(anyAppleOS 26, *)
@usableFromInline
final class QUICStreamTable<Consumer: QUICStreamConsumer & ~Copyable> {
    /// The transport state for each stream.
    @usableFromInline
    var _transportStates: QUICStreamSlots<QUICStreamTransportState>

    /// The consumer's state for each stream.
    @usableFromInline
    var _consumerStates: QUICStreamConsumerStates<Consumer.StreamState>

    /// The streams which are ready to visit.
    ///
    /// Each stream appears at most once.
    @usableFromInline
    var _ready: Deque<QUICStreamHandle>

    /// The streams the consumer hasn't yet made state for.
    @usableFromInline
    var _needsState: Deque<QUICStreamHandle>

    /// Stream handles keyed by their ID.
    @usableFromInline
    var _byID: QUICStreamIDDictionary<QUICStreamHandle>

    /// Requests that the owning connection drains this table out of band.
    @usableFromInline
    var outOfBandDrain: (() -> Void)?

    /// Whether an out-of-band drain has been requested.
    @usableFromInline
    var outOfBandDrainRequested: Bool

    /// Whether the connection is currently in a read-loop.
    ///
    /// This is used to avoid unnecessary out-of-band drain calls.
    @usableFromInline
    var inReadLoop: Bool

    /// The role of the local peer.
    let role: Role

    /// The receive windows the connection advertises, which bound the contiguous bytes a read can
    /// wait for. Set by the connection.
    var receiveWindows: ReceiveWindows = .unlimited

    /// What carries the table's streams.
    enum Backend {
        /// A connection driving the streams through its `QUICConnectionCore`. Held strongly;
        /// ``releaseBackend()`` breaks the cycle with the connection.
        case stateMachine(StateMachineQUICConnection<Consumer>)
        /// Operations without a connection behind them, for tests. Held strongly, as above.
        case test(any QUICStreamOperations)
        /// A table after ``releaseBackend()``: it has nothing to run its streams on.
        case closed
    }

    /// What carries the table's streams.
    private(set) var backend: Backend

    /// Creates a table whose streams a ``StateMachineQUICConnection`` drives.
    convenience init(role: Role, stateMachine: StateMachineQUICConnection<Consumer>) {
        self.init(role: role, backend: .stateMachine(stateMachine))
    }

    /// Creates a table whose streams run on `operations`, without a connection: for tests.
    convenience init(role: Role, operations: any QUICStreamOperations) {
        self.init(role: role, backend: .test(operations))
    }

    private init(role: Role, backend: Backend) {
        self._transportStates = QUICStreamSlots()
        self._consumerStates = QUICStreamConsumerStates()
        self._ready = []
        self._needsState = []
        self._byID = QUICStreamIDDictionary()
        self.inReadLoop = false
        self.outOfBandDrainRequested = false
        self.role = role
        self.backend = backend
    }

    deinit {
        precondition(
            self.isClosed,
            "QUICStreamTable released before closeAll(error:into:) or releaseBackend()"
        )
        // A closed table takes no new streams (`open` throws), and `closeAll` removes every
        // stream, including any opened during its drain. Streams left here never reached the
        // connection (unit tests insert slots directly): drop what they queued.
        self.abandonAllLocally()
    }

    /// The number of streams in the table.
    @inlinable
    var count: Int {
        self._transportStates.count
    }

    /// Whether a drain would have anything to report.
    @inlinable
    var hasPendingWork: Bool {
        !self._ready.isEmpty
    }

    /// Whether the table has closed: its backend is released, and it takes no new streams.
    var isClosed: Bool {
        switch self.backend {
        case .stateMachine, .test:
            return false
        case .closed:
            return true
        }
    }
}

// MARK: - Bookkeeping

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Marks the stream for the given handle as ready, enqueueing it if it isn't already enqueued.
    @inlinable
    func markReady(handle: QUICStreamHandle, events: QUICStreamEvents) {
        if let transport = self._transportStates.pointer(for: handle) {
            self._markReady(handle: handle, transport: transport, events: events)
        }  // else: handle was invalid; no-op.
    }

    @inlinable
    func _markReady(
        handle: QUICStreamHandle,
        transport: UnsafeMutablePointer<QUICStreamTransportState>,
        events: QUICStreamEvents
    ) {
        let hadNoEvents = transport.pointee.events.isEmpty
        transport.pointee.events.formUnion(events)

        if hadNoEvents && !events.isEmpty {
            self._ready.append(handle)

            if !self.inReadLoop && !self.outOfBandDrainRequested {
                self.outOfBandDrainRequested = true
                self.outOfBandDrain?()
            }
        }
    }

    /// Adds a slot for a stream, with the consumer's state for it.
    ///
    /// - Parameters:
    ///   - id: The stream ID, if the connection has already assigned one.
    ///   - state: The consumer's state for the stream. Streams which don't provide state here
    ///     will make state via ``QUICStreamConsumer/makeStreamState(_:)`` before their first visit.
    @inlinable
    func insertSlot(
        id: QUICStreamID?,
        state: consuming Consumer.StreamState?
    ) -> QUICStreamHandle {
        let transportState = QUICStreamTransportState(core: QUICStreamCore(id: id))
        let handle = self._transportStates.insert(transportState)
        let consumerStatePointer = self._consumerStates.reserve(at: handle.index)

        if let state {
            consumerStatePointer.pointee = consume state
            self._transportStates.withValue(for: handle) { $0.hasState = true }
        } else {
            self._needsState.append(handle)
        }

        if let id {
            self._byID[id] = handle
        }  // else local peer opened the stream, id is assigned later.

        return handle
    }

    /// Records the ID the connection assigned, indexes it, and connects the stream's state machine.
    @usableFromInline
    func assignID(_ id: QUICStreamID, to handle: QUICStreamHandle) {
        guard let pointer = self._transportStates.pointer(for: handle) else { return }

        switch pointer.pointee.core.connected(id: id, direction: self.direction(of: id)) {
        case .activateStream:
            ()
        case .ignoreAlreadyConnected:
            ()  // A second connected event for the same stream; the ID cannot have changed.
        case .ignoreAlreadyClosed:
            ()  // Closed while the event was in flight.
        }

        self._byID.updateValue(handle, forID: id)
    }

    /// The handle for a stream ID, or `nil` if no live stream has it.
    @usableFromInline
    func handle(forID id: QUICStreamID) -> QUICStreamHandle? {
        if let handle = self._byID[id], self._transportStates.containsValue(for: handle) {
            return handle
        } else {
            return nil
        }
    }

    /// Runs `body` on a stream and the consumer's state for it.
    ///
    /// > Important: You **MUST NOT** call `withStream` re-entrantly for the same handle. Doing so
    /// > will result in a runtime trap.
    ///
    /// - Parameters:
    ///   - handle: The handle for the stream to access.
    ///   - body: Called with the stream and state for the stream aliased by the given handle.
    ///     Called at most once. The closure won't be called if the handle doesn't alias an open
    ///     stream.
    /// - Returns: the result of `body`, or `nil` if the stream wasn't open.
    @inlinable
    func withStream<Result: ~Copyable, Failure: Error>(
        handle: QUICStreamHandle,
        execute body: (
            _ stream: inout QUICStream<Consumer>,
            _ state: inout Consumer.StreamState
        ) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        guard let pointer = self._transportStates.pointer(for: handle) else { return nil }
        guard pointer.pointee.hasState else { return nil }

        // The slot is "borrowed" for the duration of this call. Attempting to re-entrantly call
        // `withStream` will trap here. Doing this would be very surprising user behavior (they
        // are already accessing the stream in the same lexical scope) and potentially unsafe as
        // both streams would be accessing the same underlying memory for the transport and consumer
        // state.
        pointer.pointee.beginBorrow()
        defer { pointer.pointee.endBorrow() }

        let consumerState = self._consumerStates.pointer(at: handle.index)
        var stream = QUICStream<Consumer>(table: self, transport: pointer, handle: handle)
        return try body(&stream, &consumerState.pointee!)  // checked above
    }

    /// Runs `body` on the consumer's state of every stream that has one, in slot order.
    ///
    /// `body` must not reach the table: collect what it needs, and act on it afterwards.
    func forEachStreamState(_ body: (inout Consumer.StreamState) -> Void) {
        var index = QUICStreamHandle.Index.first
        while index.rawValue < self._transportStates.allocated {
            if let transport = self._transportStates.pointer(at: index), transport.pointee.hasState {
                body(&self._consumerStates.pointer(at: index).pointee!)
            }
            index.advance()
        }
    }

    /// The direction of a stream from its ID and this endpoint's role.
    private func direction(of id: QUICStreamID) -> QUICStreamDirection {
        switch id.type {
        case .clientInitiatedBidirectional, .serverInitiatedBidirectional:
            return .bidirectional
        case .clientInitiatedUnidirectional:
            return self.role == .client ? .sendOnly : .receiveOnly
        case .serverInitiatedUnidirectional:
            return self.role == .server ? .sendOnly : .receiveOnly
        }
    }
}

// MARK: - Stack events

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// The stream closed.
    func streamDisconnected(_ handle: QUICStreamHandle) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        // `.readable` as well: there may be leftover data and this will be the last chance to
        // get it.
        self._markReady(handle: handle, transport: transport, events: [.closed, .readable])
    }

    /// The peer sent RESET\_STREAM.
    func peerResetStream(_ handle: QUICStreamHandle, code: QUICApplicationErrorCode) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.core.receiveResetStream(code: code)
        transport.pointee.resetCode = code

        self._markReady(handle: handle, transport: transport, events: .reset)
    }

    /// The peer sent STOP\_SENDING to a stream, which the connection answers itself.
    func peerStoppedSending(_ handle: QUICStreamHandle, code: QUICApplicationErrorCode) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.core.receiveStopSendingAnsweredByStack(code: code)
        transport.pointee.stopSendingCode = code

        self._markReady(handle: handle, transport: transport, events: .stopSending)
    }

}

// MARK: - Draining

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Hand each ready stream to the consumer.
    ///
    func drain(into consumer: inout Consumer) {
        self.outOfBandDrainRequested = false

        while let handle = self._needsState.popFirst() {
            guard let transport = self._transportStates.pointer(for: handle) else { continue }
            assert(!transport.pointee.hasState)

            var stream = QUICStream<Consumer>(table: self, transport: transport, handle: handle)
            let consumerState = self._consumerStates.pointer(at: handle.index)
            consumerState.pointee = consumer.makeStreamState(&stream)
            transport.pointee.hasState = true
        }

        var iterator = QUICStreamIterator(table: self)
        consumer.processStreams(&iterator)
        // Finish any visit the consumer didn't pull to completion, so `QUICStreamIterator.deinit`'s
        // precondition holds once the iterator's scope ends right after this.
        iterator._finishVisit()
    }

    /// Marks every stream as closed and visits each of them, then releases the table's backend.
    ///
    /// The table's operations can't be used afterwards.
    ///
    /// - Parameters:
    ///   - error: Reported to the consumer as the reason each stream closed.
    ///   - consumer: The consumer to drain into.
    func closeAll(error: (any Error)?, into consumer: inout Consumer) {
        var index = QUICStreamHandle.Index.first
        while index.rawValue < self._transportStates.allocated {
            let transport = self._transportStates.pointer(at: index)
            let handle = self._transportStates.handle(at: index)

            if let transport, let handle {
                transport.pointee.closeError = error
                // `.readable` too: this is the last chance for the consumer to pull any data.
                self._markReady(handle: handle, transport: transport, events: [.closed, .readable])
            }

            index.advance()
        }

        self.drain(into: &consumer)
        // The connection is going away; there is nothing to tell the peer per stream.
        self.abandonAllLocally()
        self.releaseBackend()
        // Closed first, and with nothing held: the states' deinits are consumer code, and an `open`
        // from one now throws.
        self._consumerStates.removeAll()
    }

    /// Removes and returns the next stream from the ready queue.
    @inlinable
    func nextReadyHandle() -> QUICStreamHandle? {
        self._ready.popFirst()
    }

    @usableFromInline
    struct ReadySlot {
        @usableFromInline
        var transport: UnsafeMutablePointer<QUICStreamTransportState>
        @usableFromInline
        var state: UnsafeMutablePointer<Consumer.StreamState?>
        @usableFromInline
        var events: QUICStreamEvents

        @inlinable
        init(
            transport: UnsafeMutablePointer<QUICStreamTransportState>,
            state: UnsafeMutablePointer<Consumer.StreamState?>,
            events: QUICStreamEvents
        ) {
            self.transport = transport
            self.state = state
            self.events = events
        }
    }

    /// Returns the ``ReadySlot` for a handle, or `nil` if the handle is invalid.
    ///
    /// This is used only by ``QUICStreamIterator`` immediately prior to visiting the stream.
    @inlinable
    func readySlot(
        for handle: QUICStreamHandle
    ) -> ReadySlot? {
        guard let transport = self._transportStates.pointer(for: handle) else {
            // Possible if the stream gets closed before the first visit.
            return nil
        }

        // Streams only get added to the ready queue when they have events.
        precondition(!transport.pointee.events.isEmpty)
        // State is created for streams before creating the iterator (which is the only caller of
        // this function).
        precondition(transport.pointee.hasState)

        return ReadySlot(
            transport: transport,
            state: self._consumerStates.pointer(at: handle.index),
            events: transport.pointee.events
        )
    }

    /// Clears the flags a visit presented and, if that visit was the stream's last, recycles its
    /// slot.
    @usableFromInline
    func finishVisit(_ handle: QUICStreamHandle, presented: QUICStreamEvents) {
        if presented.contains(.closed) {
            self.vacateSlot(handle)
        } else if let transport = self._transportStates.pointer(for: handle) {
            // Clear the events which were shown in the last visit.
            transport.pointee.events.subtract(presented)

            // Visit didn't consume all bytes: it should be readable in the next visit.
            if transport.pointee.core.needsReadVisit {
                transport.pointee.events.insert(.readable)
            }

            if !transport.pointee.events.isEmpty {
                self._ready.append(handle)
            }
        }
    }
}

// MARK: - Slot teardown

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Closes and vacates a slot, without a visit, and releases the consumer's state for the
    /// stream only once the slot is gone: the state's deinit is consumer code.
    func vacateSlot(_ handle: QUICStreamHandle) {
        let state = self.takeConsumerState(for: handle)
        switch self.backend {
        case .stateMachine(let connection):
            self._remove(handle: handle, operations: connection)
        case .test(let operations):
            self._remove(handle: handle, operations: operations)
        case .closed:
            self._remove(handle: handle)
        }
        // Only now, with the table consistent: the state's deinit is consumer code.
        _ = consume state
    }

    /// Moves the consumer's state for `handle`'s stream out of the table, for the caller to release
    /// once the table is consistent: its deinit is consumer code.
    private func takeConsumerState(for handle: QUICStreamHandle) -> Consumer.StreamState? {
        if self._transportStates.pointer(for: handle) == nil {
            return nil
        }
        return self._consumerStates.pointer(at: handle.index).pointee.take()
    }

    /// Vacates a slot of a closed table, whose stream has nothing left to tell the connection.
    private func _remove(handle: QUICStreamHandle) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        if let id = transport.pointee.core.id {
            let removed = self._byID.removeValue(forID: id)
            assert(removed == handle)
        }
        transport.pointee.core.abandonLocally()

        self._transportStates.removeValue(for: handle)
        self._consumerStates.removeValue(at: handle.index)
    }

    /// Closes a slot's stream on `operations` and vacates the slot.
    private func _remove(handle: QUICStreamHandle, operations: some QUICStreamOperations) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        if let id = transport.pointee.core.id {
            let removed = self._byID.removeValue(forID: id)
            assert(removed == handle)
            transport.pointee.core.close(on: operations, id: id)
        } else {
            operations.cancelPendingOpen(handle)
            transport.pointee.core.abandonLocally()
        }

        self._transportStates.removeValue(for: handle)
        self._consumerStates.removeValue(at: handle.index)
    }

    /// Vacates every remaining slot without telling the connection, dropping what each queued.
    /// Consumer states are the caller's to release.
    private func abandonAllLocally() {
        self._transportStates.removeAll { transport in
            var transport = consume transport
            transport.core.abandonLocally()
        }
        self._ready.removeAll(keepingCapacity: true)
        self._needsState.removeAll(keepingCapacity: true)
        self._byID.removeAll()
    }
}

// MARK: - Closing

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Releases the table's backend, after which it takes no new streams and its operations trap.
    /// Idempotent.
    func releaseBackend() {
        switch self.backend {
        case .stateMachine, .test:
            // Breaks the cycle with the connection.
            self.backend = .closed
        case .closed:
            ()
        }
    }
}

// MARK: - Stream operations

/// `QUICStream`'s operations that reach the connection.
@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Reads from the stream `transport` belongs to.
    @usableFromInline
    func read(
        _ transport: UnsafeMutablePointer<QUICStreamTransportState>,
        minContiguous: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        let id = transport.pointee.core.id
        let minContiguous =
            id.map {
                min(minContiguous, self.receiveWindows.largestMinContiguous(for: $0, role: self.role))
            } ?? minContiguous
        switch self.backend {
        case .stateMachine(let connection):
            guard let id else { return .nothingAvailable }
            return transport.pointee.core.read(minContiguous: minContiguous, from: connection, id: id, body)
        case .test(let operations):
            guard let id else { return .nothingAvailable }
            return transport.pointee.core.read(minContiguous: minContiguous, from: operations, id: id, body)
        case .closed:
            preconditionFailure("stream table used after it was closed")
        }
    }

    /// How many bytes the stream `transport` belongs to will accept right now.
    @usableFromInline
    func writableBytes(_ transport: UnsafeMutablePointer<QUICStreamTransportState>) -> Int {
        switch self.backend {
        case .stateMachine(let connection):
            return transport.pointee.core.writableBytes(on: connection)
        case .test(let operations):
            return transport.pointee.core.writableBytes(on: operations)
        case .closed:
            preconditionFailure("stream table used after it was closed")
        }
    }

    /// Writes the queued data of the stream `transport` belongs to.
    @usableFromInline
    func flush(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, fin: Bool) throws {
        switch self.backend {
        case .stateMachine(let connection):
            try transport.pointee.core.flush(fin: fin, to: connection)
        case .test(let operations):
            try transport.pointee.core.flush(fin: fin, to: operations)
        case .closed:
            preconditionFailure("stream table used after it was closed")
        }
    }

    /// Resets the send side of the stream `transport` belongs to.
    @usableFromInline
    func sendResetStream(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, code: QUICApplicationErrorCode) {
        switch self.backend {
        case .stateMachine(let connection):
            transport.pointee.core.sendResetStream(code: code, on: connection)
        case .test(let operations):
            transport.pointee.core.sendResetStream(code: code, on: operations)
        case .closed:
            preconditionFailure("stream table used after it was closed")
        }
    }

    /// Closes the receive side of the stream `transport` belongs to.
    @usableFromInline
    func sendStopSending(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, code: QUICApplicationErrorCode) {
        switch self.backend {
        case .stateMachine(let connection):
            transport.pointee.core.sendStopSending(code: code, on: connection)
        case .test(let operations):
            transport.pointee.core.sendStopSending(code: code, on: operations)
        case .closed:
            preconditionFailure("stream table used after it was closed")
        }
    }
}

/// The flow control windows a connection advertises for what it receives (RFC 9000 Section 4).
@available(anyAppleOS 26, *)
struct ReceiveWindows {
    /// The window of a bidirectional stream the local peer opened.
    var bidirectionalLocal: Int
    /// The window of a bidirectional stream the remote peer opened.
    var bidirectionalRemote: Int
    /// The window of a unidirectional stream the remote peer opened.
    var unidirectional: Int
    /// The window of the whole connection.
    var connection: Int

    /// No bound.
    static var unlimited: Self {
        Self(bidirectionalLocal: .max, bidirectionalRemote: .max, unidirectional: .max, connection: .max)
    }

    init(bidirectionalLocal: Int, bidirectionalRemote: Int, unidirectional: Int, connection: Int) {
        self.bidirectionalLocal = bidirectionalLocal
        self.bidirectionalRemote = bidirectionalRemote
        self.unidirectional = unidirectional
        self.connection = connection
    }

    init(_ configuration: QUICConfiguration) {
        self.init(
            bidirectionalLocal: configuration.initialMaxStreamDataBidiLocal,
            bidirectionalRemote: configuration.initialMaxStreamDataBidiRemote,
            unidirectional: configuration.initialMaxStreamDataUni,
            connection: configuration.initialMaxData
        )
    }

    /// The most contiguous bytes a read on the stream `id` can wait for.
    ///
    /// The peer can't send past a window, and the window only moves once the application consumes
    /// half of it (RFC 9000 Section 4.2). A read that waits for more than half the window may never
    /// see it arrive; half of the smaller of the stream's and the connection's windows always can.
    func largestMinContiguous(for id: QUICStreamID, role: Role) -> Int {
        let stream: Int
        switch (QUICStreamType(id), role) {
        case (.clientInitiatedBidirectional, .client), (.serverInitiatedBidirectional, .server):
            stream = self.bidirectionalLocal
        case (.clientInitiatedBidirectional, .server), (.serverInitiatedBidirectional, .client):
            stream = self.bidirectionalRemote
        case (.clientInitiatedUnidirectional, _), (.serverInitiatedUnidirectional, _):
            // The local peer never reads from a stream it opened unidirectionally.
            stream = self.unidirectional
        }
        return max(min(stream, self.connection) / 2, 1)
    }
}
