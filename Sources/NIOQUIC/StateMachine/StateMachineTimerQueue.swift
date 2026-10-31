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
@_spi(Essentials) import QUICStateMachine

/// What a ``StateMachineTimerQueue`` calls once the deadline it armed is due.
@available(anyAppleOS 26, *)
protocol StateMachineTimerTarget: AnyObject {
    /// The deadline armed is due. The slot is gone: the target arms a new one for what is left.
    func timerFired()
}

/// The timers of the state machine connections of one ``QUICHandler``, so of one event loop, served
/// by one scheduled NIO task.
///
/// Each connection arms at most one deadline, in a slot. The queue keeps the deadlines in a binary
/// min-heap and schedules a NIO task for the earliest; when it fires, every target due by then is
/// called. As with a connection's own task before, a deadline moved later keeps the earlier one,
/// which fires early, and the target arms again for the later one. A deadline moved earlier adds
/// a heap entry and leaves the old one behind, stale: the queue skips stale entries, and drops
/// them all once they outnumber the live ones.
///
/// A slot holds its target from the time it is armed until it fires or is cancelled, as the NIO
/// task did: a connection whose channel went away keeps closing on its timer.
@available(anyAppleOS 26, *)
final class StateMachineTimerQueue {
    /// An armed deadline. It is valid until it fires or is cancelled; then the queue may hand its
    /// storage to another target, and treats this one as not armed.
    struct Slot: Equatable {
        fileprivate var index: Int
        fileprivate var generation: UInt32
    }

    private struct Storage {
        /// Changes whenever the slot is freed, which makes its `Slot`s and heap entries stale.
        var generation: UInt32 = 0
        /// The deadline, in nanoseconds since `NetworkClock.Instant.zero`, while armed.
        var deadline: Int64 = 0
        /// What the slot calls; set while armed.
        var target: (any StateMachineTimerTarget)?
    }

    private struct Entry {
        var deadline: Int64
        var index: Int
        var generation: UInt32
    }

    private let eventLoop: any EventLoop
    private var slots: [Storage] = []
    /// The slots not armed.
    private var freeSlots: [Int] = []
    /// The armed slots.
    private var armedCount = 0
    /// A min-heap of deadlines by `Entry.deadline`. An entry is live if its slot is still armed
    /// for its deadline.
    private var heap: [Entry] = []
    /// The entries due when the task fired, which `fire()` calls one after the other.
    private var due: [Entry] = []
    private var isFiring = false

    private var task: Scheduled<Void>?
    /// When `task` fires; `Int64.max` without one.
    private var taskDeadline = Int64.max

    #if DEBUG
    /// The NIO tasks the queue scheduled.
    private(set) var _forTesting_tasksScheduled = 0
    /// The times the queue dropped its stale entries.
    private(set) var _forTesting_compactions = 0

    /// Whether a NIO task is scheduled.
    var _forTesting_isTaskScheduled: Bool {
        self.task != nil
    }

    var _forTesting_armedSlots: Int {
        self.armedCount
    }

    /// The heap's entries, live and stale.
    var _forTesting_heapCount: Int {
        self.heap.count
    }
    #endif

    init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
    }

    // MARK: Clock

    /// `instant` in nanoseconds, the uptime of its NIO deadline: the connections run their cores
    /// on NIO's clock.
    private static func nanoseconds(_ instant: NetworkClock.Instant) -> Int64 {
        NetworkClock.Instant.zero.duration(to: instant).nanoseconds
    }

    // MARK: Arming

    /// Whether `slot` is armed.
    func isArmed(_ slot: Slot) -> Bool {
        slot.index < self.slots.count && self.slots[slot.index].generation == slot.generation
            && self.slots[slot.index].target != nil
    }

    /// Arms `slot` for `deadline`, or a new slot for `target` if `slot` is `nil` or not armed, and
    /// returns the slot armed.
    ///
    /// A slot armed for an earlier deadline keeps it: it fires early, and the target arms again.
    func schedule(
        _ slot: Slot?,
        at deadline: NetworkClock.Instant,
        target: any StateMachineTimerTarget
    ) -> Slot {
        let nanoseconds = Self.nanoseconds(deadline)
        let armed: Slot
        if let slot, self.isArmed(slot) {
            guard nanoseconds < self.slots[slot.index].deadline else { return slot }
            self.slots[slot.index].deadline = nanoseconds
            armed = slot
        } else {
            let index: Int
            if let free = self.freeSlots.popLast() {
                index = free
            } else {
                index = self.slots.count
                self.slots.append(Storage())
            }
            self.slots[index].deadline = nanoseconds
            self.slots[index].target = target
            self.armedCount += 1
            armed = Slot(index: index, generation: self.slots[index].generation)
        }
        self.push(Entry(deadline: nanoseconds, index: armed.index, generation: armed.generation))
        // `fire()` schedules the task for what is left once it called every due target.
        if !self.isFiring && nanoseconds < self.taskDeadline {
            self.scheduleTask(at: nanoseconds)
        }
        return armed
    }

    /// Disarms `slot`, which releases its target. Does nothing if it isn't armed.
    func cancel(_ slot: Slot) {
        guard self.isArmed(slot) else { return }
        // Released once this returns.
        let target = self.free(slot.index)
        if self.armedCount == 0 {
            self.cancelTask()
            self.heap.removeAll(keepingCapacity: true)
        }
        _ = target
    }

    /// Disarms every slot and cancels the NIO task: the handler is going away. Slots armed later
    /// work as before.
    func cancelAll() {
        self.cancelTask()
        // The targets are released once this returns, when nothing accesses the slots any more.
        var targets: [any StateMachineTimerTarget] = []
        targets.reserveCapacity(self.armedCount)
        for index in self.slots.indices where self.slots[index].target != nil {
            targets.append(self.free(index))
        }
        self.heap.removeAll()
        self.due.removeAll()
        _ = targets
    }

    /// Frees an armed slot and returns its target.
    private func free(_ index: Int) -> any StateMachineTimerTarget {
        // '!' is fine: the slot is armed.
        let target = self.slots[index].target.take()!
        self.slots[index].generation &+= 1
        self.freeSlots.append(index)
        self.armedCount -= 1
        return target
    }

    // MARK: Firing

    private func scheduleTask(at deadline: Int64) {
        self.task?.cancel()
        self.taskDeadline = deadline
        self.task = self.eventLoop.assumeIsolated().scheduleTask(
            deadline: .uptimeNanoseconds(UInt64(max(0, deadline)))
        ) {
            self.fire()
        }
        #if DEBUG
        self._forTesting_tasksScheduled += 1
        #endif
    }

    private func cancelTask() {
        self.task?.cancel()
        self.task = nil
        self.taskDeadline = .max
    }

    private func fire() {
        self.task = nil
        self.taskDeadline = .max
        let now = Int64(self.eventLoop.now.uptimeNanoseconds)
        // Take every entry due first: one a target adds while it runs waits for the next task,
        // even if it is due, so a target that keeps arming for now can't hold the loop.
        while let first = self.heap.first, first.deadline <= now {
            self.popFirst()
            if self.isLive(first) {
                self.due.append(first)
            }
        }
        self.isFiring = true
        var next = 0
        while next < self.due.count {
            let entry = self.due[next]
            next += 1
            // An earlier target may have cancelled this slot or moved its deadline.
            guard self.isLive(entry) else { continue }
            self.free(entry.index).timerFired()
        }
        self.isFiring = false
        self.due.removeAll(keepingCapacity: true)
        while let first = self.heap.first, !self.isLive(first) {
            self.popFirst()
        }
        if let first = self.heap.first {
            self.scheduleTask(at: first.deadline)
        }
    }

    private func isLive(_ entry: Entry) -> Bool {
        self.slots[entry.index].generation == entry.generation && self.slots[entry.index].target != nil
            && self.slots[entry.index].deadline == entry.deadline
    }

    // MARK: Heap

    private func push(_ entry: Entry) {
        // Every armed slot has one live entry; drop the stale ones once they are the majority.
        if self.heap.count >= 64, self.heap.count > 2 &* self.armedCount {
            self.compact()
        }
        self.heap.append(entry)
        Self.siftUp(&self.heap, from: self.heap.count - 1)
    }

    private func popFirst() {
        let last = self.heap.removeLast()
        guard !self.heap.isEmpty else { return }
        self.heap[0] = last
        Self.siftDown(&self.heap, from: 0)
    }

    private func compact() {
        self.heap.removeAll { !self.isLive($0) }
        var index = self.heap.count / 2
        while index > 0 {
            index -= 1
            Self.siftDown(&self.heap, from: index)
        }
        #if DEBUG
        self._forTesting_compactions += 1
        #endif
    }

    private static func siftUp(_ heap: inout [Entry], from index: Int) {
        var child = index
        let entry = heap[child]
        while child > 0 {
            let parent = (child - 1) / 2
            guard entry.deadline < heap[parent].deadline else { break }
            heap[child] = heap[parent]
            child = parent
        }
        heap[child] = entry
    }

    private static func siftDown(_ heap: inout [Entry], from index: Int) {
        var parent = index
        let entry = heap[parent]
        while true {
            var child = 2 &* parent &+ 1
            guard child < heap.count else { break }
            if child + 1 < heap.count, heap[child + 1].deadline < heap[child].deadline {
                child += 1
            }
            guard heap[child].deadline < entry.deadline else { break }
            heap[parent] = heap[child]
            parent = child
        }
        heap[parent] = entry
    }
}
