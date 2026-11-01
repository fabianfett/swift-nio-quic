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
import NIOEmbedded
import NIOQUICHelpers
import Testing

@_spi(Essentials) @_spi(ProtocolProvider) @testable import NIOQUIC

@available(anyAppleOS 26, *)
private struct TestConsumer: QUICStreamConsumer {
    typealias StreamState = Int

    mutating func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) -> Int {
        Issue.record("state requested for a table which is never drained")
        return 0
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        Issue.record("drained a table which is never drained")
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    fileprivate func events(for handle: QUICStreamHandle) -> QUICStreamEvents? {
        self._transportStates.pointer(for: handle)?.pointee.events
    }
}

@available(anyAppleOS 26, *)
private struct RecordingConsumer: QUICStreamConsumer {
    struct StreamState {
        /// Which ``makeStreamState(_:)`` call produced this, counting from one.
        var madeAt: Int
    }

    struct Sighting: Equatable {
        var handle: QUICStreamHandle
        var events: QUICStreamEvents
        var stateMadeAt: Int
    }

    init(
        pullLimit: Int? = nil,
        onVisit: ((_ visit: consuming QUICStreamVisit<Self>) -> Void)? = nil
    ) {
        self.pullLimit = pullLimit
        self.onVisit = onVisit
        self.madeStates = 0
        self.sightings = []
    }

    /// How many states have been made.
    var madeStates: Int

    /// One entry per visit, across every drain.
    var sightings: [Sighting]

    /// The most visits to pull per drain, or `nil` to pull them all.
    var pullLimit: Int?

    /// Called for each visit.
    var onVisit: ((_ visit: consuming QUICStreamVisit<Self>) -> Void)?

    mutating func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) -> StreamState {
        self.madeStates &+= 1
        return StreamState(madeAt: self.madeStates)
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        var pulled = 0

        while self.pullLimit != pulled, let visit = streams.next() {
            pulled &+= 1

            let sighting = Sighting(
                handle: visit.handle,
                events: visit.events,
                stateMadeAt: visit.state.madeAt
            )

            self.sightings.append(sighting)

            if let onVisit = self.onVisit {
                onVisit(visit)
            }
        }
    }
}

private struct Boom: Error {}

@Suite
struct QUICStreamTableTests {
    @available(anyAppleOS 26, *)
    private static func makeTable(role: Role = .client) -> QUICStreamTable<TestConsumer> {
        TestStreamOperations.makeTable(role: role).0
    }

    @available(anyAppleOS 26, *)
    private static func makeRecordingTable(role: Role = .client) -> QUICStreamTable<RecordingConsumer> {
        TestStreamOperations.makeTable(role: role).0
    }

    // MARK: - ID indexing

    @available(anyAppleOS 26, *)
    @Test
    func insertWithIDIsIndexedByID() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: 4, state: 1)
        #expect(table.handle(forID: 4) == handle)
        #expect(table.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func insertWithoutIDIsNotIndexedUntilAssigned() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)
        #expect(table.handle(forID: 4) == nil)

        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == handle)

        let id = table.withStream(handle: handle) { stream, _ in stream.id }
        #expect(id == 4)
    }

    @available(anyAppleOS 26, *)
    @Test
    func idLookupFailsOnceTheSlotIsVacated() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: 4, state: 1)
        table.vacateSlot(handle)
        #expect(table.handle(forID: 4) == nil)
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func assignIDToVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.vacateSlot(handle)
        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == nil)
    }

    // MARK: - Generations

    @available(anyAppleOS 26, *)
    @Test
    func eventsForARecycledSlotAreDroppedForTheStaleHandle() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let stale = table.insertSlot(id: nil, state: 1)
        table.vacateSlot(stale)

        // 'live' should reuse the slot that 'stale' used.
        let live = table.insertSlot(id: nil, state: 2)
        #expect(stale.index == live.index)
        #expect(stale.generation != live.generation)

        table.markReady(handle: stale, events: .readable)
        #expect(table.events(for: stale) == nil)
        #expect(table.events(for: live) == [])
    }

    // MARK: - Ready queue

    @available(anyAppleOS 26, *)
    @Test
    func repeatedMarkReadyEnqueuesOnceAndUnionsEvents() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)

        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: .writable)

        #expect(table.hasPendingWork)
        #expect(table.events(for: handle) == [.readable, .writable])
    }

    @available(anyAppleOS 26, *)
    @Test
    func markReadyWithNoEventsDoesNotEnqueue() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.markReady(handle: handle, events: [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 26, *)
    @Test
    func markReadyOnAVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)

        table.vacateSlot(handle)
        table.markReady(handle: handle, events: .readable)

        #expect(!table.hasPendingWork)
    }

    // MARK: - Consumer state

    @available(anyAppleOS 26, *)
    @Test
    func slotWithoutStateIsNotVisited() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: nil)
        #expect(table.withStream(handle: handle) { _, state in state } == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func stateMutationsPersistBetweenVisits() {
        let table = Self.makeTable()
        defer { table.releaseBackend() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.withStream(handle: handle) { _, state in state += 1 }
        #expect(table.withStream(handle: handle) { _, state in state } == 2)
    }

    @available(anyAppleOS 26, *)
    @Test
    func nestedVisitsToOneStreamTrap() async throws {
        #if os(Android)
        try Test.cancel("Exit tests cannot be run on Android yet")
        #else
        await #expect(processExitsWith: .failure) {
            let table = Self.makeTable()
            let handle = table.insertSlot(id: nil, state: 1)

            table.withStream(handle: handle) { _, _ in
                table.withStream(handle: handle) { _, _ in
                }
            }
        }
        #endif
    }

    // MARK: - Draining

    @available(anyAppleOS 26, *)
    @Test
    func readyStreamIsVisitedOnceAndNotAgain() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)

        table.drain(into: &consumer)
        let sighting = RecordingConsumer.Sighting(handle: handle, events: .readable, stateMadeAt: 1)
        #expect(consumer.sightings == [sighting])

        // Shouldn't be seen a second time.
        table.drain(into: &consumer)
        #expect(consumer.sightings == [sighting])
    }

    @available(anyAppleOS 26, *)
    @Test
    func visitedOncePerTick() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: [.stopSending, .writable])

        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == [.readable, .writable, .stopSending])
    }

    @available(anyAppleOS 26, *)
    @Test
    func ignoredVisitIsOfferedAgain() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()

        // Don't pull any streams.
        consumer.pullLimit = 0
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)

        // Run the first drain.
        table.drain(into: &consumer)
        #expect(consumer.sightings.isEmpty)

        // Remove the pull limit and drain again.
        consumer.pullLimit = nil
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == .readable)
    }

    @available(anyAppleOS 26, *)
    @Test
    func lastVisitIsFinishedWhenTheConsumerNeverSeesNil() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer(pullLimit: 1)
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)

        // The visit should've been finished by the iterator: on events, and no pending work.
        #expect(table.events(for: handle) == [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 26, *)
    @Test
    func makeStreamStateIsCalledOncePerStream() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 1)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [1, 1])
    }

    @available(anyAppleOS 26, *)
    @Test
    func slotInsertedWithStateIsNotOfferedToTheConsumer() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: RecordingConsumer.StreamState(madeAt: 42))
        table.markReady(handle: handle, events: [.opened, .readable])
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 0)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [42])
    }

    @available(anyAppleOS 26, *)
    @Test
    func nestedAccessToAnotherStreamFromInsideAVisit() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let first = table.insertSlot(id: 0, state: nil)
        let second = table.insertSlot(id: 4, state: nil)

        // Reach the second stream from inside every visit, which is the case the exclusivity rules
        // make impossible through the slot store's closure-taking members.
        var visitedBy: [Int] = []
        consumer.onVisit = { visit in
            var streams = visit.streams
            let outer = visit.state.madeAt

            if let inner = streams.withStream(handle: second, execute: { _, state in state.madeAt }) {
                visitedBy.append(outer)
                visitedBy.append(inner)
            }
        }

        table.markReady(handle: first, events: .readable)
        table.markReady(handle: second, events: .readable)
        table.drain(into: &consumer)

        #expect(consumer.sightings.count == 2)
        // Each visit sees two streams: the outer stream (first and second) and then the second
        // stream.
        #expect(visitedBy == [1, 2, 2, 2])
    }

    @available(anyAppleOS 26, *)
    @Test
    func withStreamHandsOverTheStreamAndItsStateTogether() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        consumer.onVisit = { visit in
            var visit = consume visit
            visit.withStream { stream, state in
                stream.write(ByteBuffer(string: "hello"))
                try? stream.flush()
                state.madeAt &+= 100
            }
        }

        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)

        consumer.onVisit = nil
        table.markReady(handle: handle, events: .writable)
        table.drain(into: &consumer)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [1, 101])
    }

    // MARK: - Closing

    @available(anyAppleOS 26, *)
    @Test
    func closedIsDeliveredOnceAndRecyclesTheSlot() {
        let table = Self.makeRecordingTable()
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        var takenStates: [Int] = []
        var reachedAfterTake: [Int?] = []
        consumer.onVisit = { visit in
            guard let state = visit.finish() else { return }

            takenStates.append(state.madeAt)

            // The state's been taken but the handle is still valid (the visit hasn't finished
            // yet). Reaching back through the table shouldn't give us access to the stream
            // (i.e. this should return `nil`).
            let madeAt = table.withStream(handle: handle) { _, state in state.madeAt }
            reachedAfterTake.append(madeAt)
        }

        table.markReady(handle: handle, events: [.readable, .closed])
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == [.readable, .closed])
        #expect(takenStates == [1])
        #expect(reachedAfterTake == [nil])
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeAllDeliversClosedForEveryLiveStream() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let first = table.insertSlot(id: 0, state: nil)
        let second = table.insertSlot(id: 4, state: nil)
        let third = table.insertSlot(id: 8, state: nil)
        table.markReady(handle: second, events: .readable)
        table.closeAll(error: Boom(), into: &consumer)

        #expect(consumer.sightings.count == 3)
        // Second is enqueued first (when marked as ready), first and third are added to the
        // queue by 'closeAll'.
        #expect(consumer.sightings.map { $0.handle } == [second, first, third])
        #expect(consumer.sightings.allSatisfy { $0.events.contains(.closed) })
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeAllRecyclesSlotsTheConsumerNeverPulls() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer(pullLimit: 1)

        _ = table.insertSlot(id: 0, state: nil)
        _ = table.insertSlot(id: 4, state: nil)  // ignored because of pullLimit

        table.closeAll(error: Boom(), into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(table.count == 0)
    }

    // MARK: - Opening

    @available(anyAppleOS 26, *)
    @Test(
        arguments: [
            (Role.client, QUICStreamType.serverInitiatedBidirectional),
            (.client, .serverInitiatedUnidirectional),
            (.server, .clientInitiatedBidirectional),
            (.server, .clientInitiatedUnidirectional),
        ]
    )
    func openRejectsAStreamTypeThisRoleCannotInitiate(role: Role, type: QUICStreamType) {
        let table = Self.makeRecordingTable(role: role)
        defer { table.releaseBackend() }

        #expect(throws: QUICError.invalidStreamTypeForRole) {
            _ = try table.open(type, state: .init(madeAt: 1))
        }
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [Role.client, .server], [true, false])
    func openAssignsAnIDAndReportsTheStreamOpened(role: Role, bidirectional: Bool) throws {
        let (table, operations) = TestStreamOperations<RecordingConsumer>.makeTable(role: role)
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()

        let type: QUICStreamType
        switch (role, bidirectional) {
        case (.client, true): type = .clientInitiatedBidirectional
        case (.client, false): type = .clientInitiatedUnidirectional
        case (.server, true): type = .serverInitiatedBidirectional
        case (.server, false): type = .serverInitiatedUnidirectional
        }
        let first = try table.open(type, state: .init(madeAt: 1))
        let second = try table.open(type, state: .init(madeAt: 2))

        let firstID = try #require(table.withStream(handle: first) { stream, _ in stream.id } ?? nil)
        let secondID = try #require(table.withStream(handle: second) { stream, _ in stream.id } ?? nil)
        #expect(QUICStreamType(firstID) == type)
        #expect(secondID.rawValue == firstID.rawValue + 4)
        #expect(table.handle(forID: firstID) == first)
        #expect(operations[firstID] != nil)

        table.drain(into: &consumer)
        #expect(consumer.sightings.map(\.handle) == [first, second])
        #expect(consumer.sightings.allSatisfy { $0.events == .opened })
        #expect(consumer.madeStates == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func openBeyondTheStreamLimitWaitsForTheLimitToRise() throws {
        let (table, operations) = TestStreamOperations<RecordingConsumer>.makeTable(
            role: .client,
            bidirectionalLimit: 1
        )
        defer { table.releaseBackend() }
        var consumer = RecordingConsumer()

        let first = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        let second = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 2))
        #expect(table.withStream(handle: second) { stream, _ in stream.id } == .some(nil))
        #expect(operations.pendingOpens.map(\.handle) == [second])

        // A flush before the ID arrives is refused, and keeps what was written.
        table.withStream(handle: second) { stream, _ in
            stream.write(ByteBuffer(string: "early"))
            #expect(throws: (any Error).self) { try stream.flush() }
        }

        table.drain(into: &consumer)
        #expect(consumer.sightings.map(\.handle) == [first])

        operations.raiseLimit(bidirectional: true, by: 1)
        let id = try #require(table.withStream(handle: second) { stream, _ in stream.id } ?? nil)
        #expect(id == 4)
        try table.withStream(handle: second) { stream, _ in try stream.flush(fin: true) }
        #expect(operations[id]?.outboundString == "early")
        #expect(operations[id]?.finished == true)

        table.drain(into: &consumer)
        #expect(consumer.sightings.map(\.handle) == [first, second])
    }

    @available(anyAppleOS 26, *)
    @Test
    func vacatingAStreamWaitingForTheLimitCancelsItsOpen() throws {
        let (table, operations) = TestStreamOperations<RecordingConsumer>.makeTable(
            role: .client,
            bidirectionalLimit: 0
        )
        defer { table.releaseBackend() }

        let handle = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        #expect(operations.pendingOpens.count == 1)
        table.vacateSlot(handle)
        #expect(operations.pendingOpens.isEmpty)
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func openAfterCloseAllThrowsAndAddsNothing() {
        let table = Self.makeRecordingTable(role: .client)
        var consumer = RecordingConsumer()
        table.closeAll(error: nil, into: &consumer)

        #expect(throws: QUICError.invalidStreamState) {
            _ = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        }
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closingAStreamStopsBothDirectionsLeftOpen() throws {
        let (table, operations) = TestStreamOperations<RecordingConsumer>.makeTable(role: .client)
        defer { table.releaseBackend() }

        let handle = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        let id = try #require(table.withStream(handle: handle) { stream, _ in stream.id } ?? nil)
        table.vacateSlot(handle)

        #expect(operations[id]?.resetCode == QUICApplicationErrorCode(0))
        #expect(operations[id]?.stopSendingCode == QUICApplicationErrorCode(0))
    }
}
