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

#if DEBUG
import NIOCore
import NIOEmbedded
@_spi(Essentials) import QUICStateMachine
import Testing

@testable import NIOQUIC

/// Records when it fires, in a log it shares with other targets, and runs `onFire` then.
@available(anyAppleOS 26, *)
private final class Target: StateMachineTimerTarget {
    final class Log {
        var fired: [(name: String, at: NIODeadline)] = []
        let eventLoop: EmbeddedEventLoop

        init(eventLoop: EmbeddedEventLoop) {
            self.eventLoop = eventLoop
        }

        var names: [String] {
            self.fired.map(\.name)
        }
    }

    let name: String
    let log: Log
    var slot: StateMachineTimerQueue.Slot?
    var onFire: (() -> Void)?
    var onDeinit: (() -> Void)?

    init(_ name: String, log: Log) {
        self.name = name
        self.log = log
    }

    deinit {
        self.onDeinit?()
    }

    func timerFired() {
        self.slot = nil
        self.log.fired.append((self.name, self.log.eventLoop.now))
        self.onFire?()
    }
}

@Suite(.timeLimit(.minutes(1)))
struct StateMachineTimerQueueTests {
    /// The instant `milliseconds` after the start of an `EmbeddedEventLoop`'s clock.
    @available(anyAppleOS 26, *)
    private static func at(_ milliseconds: Int64) -> NetworkClock.Instant {
        NetworkClock.Instant.zero.advanced(by: .milliseconds(milliseconds))
    }

    private static func deadline(_ milliseconds: Int64) -> NIODeadline {
        .uptimeNanoseconds(UInt64(milliseconds) * 1_000_000)
    }

    @available(anyAppleOS 26, *)
    private static func arm(
        _ target: Target,
        at milliseconds: Int64,
        in queue: StateMachineTimerQueue
    ) {
        target.slot = queue.schedule(target.slot, at: Self.at(milliseconds), target: target)
    }

    @available(anyAppleOS 26, *)
    @Test func targetsFireInDeadlineOrderFromOneTask() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let targets = (0..<100).map { Target("\($0)", log: log) }
        // The deadlines in a shuffled order, none earlier than the first.
        var order = Array(1..<100).shuffled()
        order.insert(0, at: 0)
        for index in order {
            Self.arm(targets[index], at: 10 + Int64(index), in: queue)
        }

        #expect(queue._forTesting_tasksScheduled == 1)
        #expect(queue._forTesting_isTaskScheduled)
        #expect(queue._forTesting_armedSlots == 100)

        eventLoop.advanceTime(to: Self.deadline(59))
        #expect(log.names == (0..<50).map { "\($0)" })
        #expect(log.fired.allSatisfy { $0.at == Self.deadline(10 + Int64($0.name)!) })
        #expect(queue._forTesting_armedSlots == 50)
        eventLoop.advanceTime(to: Self.deadline(200))
        #expect(log.names == (0..<100).map { "\($0)" })
        #expect(!queue._forTesting_isTaskScheduled)
        #expect(queue._forTesting_armedSlots == 0)
        // One task for each deadline, as each target fires on its own tick.
        #expect(queue._forTesting_tasksScheduled == 100)
    }

    @available(anyAppleOS 26, *)
    @Test func aDeadlineMovedEarlierFiresThenAndReschedulesTheTask() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let first = Target("first", log: log)
        let second = Target("second", log: log)
        Self.arm(first, at: 20, in: queue)
        Self.arm(second, at: 30, in: queue)
        #expect(queue._forTesting_tasksScheduled == 1)

        // Earlier than the task: the task moves.
        Self.arm(second, at: 10, in: queue)
        #expect(queue._forTesting_tasksScheduled == 2)
        // Earlier than its own deadline, but not the task's.
        Self.arm(first, at: 15, in: queue)
        #expect(queue._forTesting_tasksScheduled == 2)

        eventLoop.advanceTime(to: Self.deadline(10))
        #expect(log.names == ["second"])
        eventLoop.advanceTime(to: Self.deadline(15))
        #expect(log.names == ["second", "first"])
        eventLoop.advanceTime(to: Self.deadline(100))
        // The deadlines the slots moved away from don't fire.
        #expect(log.names == ["second", "first"])
        #expect(queue._forTesting_heapCount == 0)
    }

    @available(anyAppleOS 26, *)
    @Test func aDeadlineMovedLaterFiresEarlyAndTheTargetArmsAgain() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let target = Target("target", log: log)
        Self.arm(target, at: 10, in: queue)
        let slot = target.slot

        Self.arm(target, at: 20, in: queue)
        #expect(target.slot == slot)
        #expect(queue._forTesting_heapCount == 1)
        #expect(queue._forTesting_tasksScheduled == 1)

        // The target arms for the later deadline, as a connection does.
        var rearmed = false
        target.onFire = {
            guard !rearmed else { return }
            rearmed = true
            Self.arm(target, at: 20, in: queue)
        }
        eventLoop.advanceTime(to: Self.deadline(10))
        #expect(log.fired.map(\.at) == [Self.deadline(10)])
        #expect(target.slot != nil)
        eventLoop.advanceTime(to: Self.deadline(30))
        #expect(log.fired.map(\.at) == [Self.deadline(10), Self.deadline(20)])
    }

    /// A connection arms its next deadline from `timerFired()`, which the queue fires in a task of
    /// its own.
    @available(anyAppleOS 26, *)
    @Test func aTargetArmingAgainFromTimerFiredFiresAgain() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let target = Target("target", log: log)
        target.onFire = {
            if log.fired.count < 3 {
                Self.arm(target, at: Int64(log.fired.count) * 100 + 10, in: queue)
            }
        }
        Self.arm(target, at: 10, in: queue)

        eventLoop.advanceTime(to: Self.deadline(10))
        #expect(log.fired.map(\.at) == [Self.deadline(10)])
        #expect(queue._forTesting_isTaskScheduled)
        eventLoop.advanceTime(to: Self.deadline(1000))
        #expect(log.fired.map(\.at) == [Self.deadline(10), Self.deadline(110), Self.deadline(210)])
        #expect(!queue._forTesting_isTaskScheduled)
        #expect(queue._forTesting_tasksScheduled == 3)
    }

    /// The loss recovery timer moves its deadline later on nearly every packet: that schedules no
    /// NIO task, only the early firing does once. A slot cancelled after its deadline moved later
    /// doesn't fire at either.
    @available(anyAppleOS 26, *)
    @Test func pushingADeadlineOutSchedulesNoTask() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let target = Target("target", log: log)
        var wanted: Int64 = 0
        for step in 1...20 {
            wanted = 100 + Int64(step) * 10
            Self.arm(target, at: wanted, in: queue)
        }
        let cancelled = Target("cancelled", log: log)
        Self.arm(cancelled, at: 150, in: queue)
        Self.arm(cancelled, at: 300, in: queue)
        queue.cancel(try #require(cancelled.slot))
        #expect(queue._forTesting_tasksScheduled == 1)

        // The first deadline fires early, and the target arms for what is left, as a connection
        // does.
        target.onFire = {
            if log.eventLoop.now < Self.deadline(wanted) {
                Self.arm(target, at: wanted, in: queue)
            }
        }
        eventLoop.advanceTime(to: Self.deadline(110))
        #expect(log.fired.map(\.at) == [Self.deadline(110)])
        #expect(queue._forTesting_tasksScheduled == 2)
        eventLoop.advanceTime(to: Self.deadline(1000))
        #expect(log.fired.map(\.at) == [Self.deadline(110), Self.deadline(300)])
        #expect(queue._forTesting_tasksScheduled == 2)
    }

    @available(anyAppleOS 26, *)
    @Test func targetsWithTheSameDeadlineFireInOneTask() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let targets = (0..<3).map { Target("\($0)", log: log) }
        for target in targets {
            Self.arm(target, at: 10, in: queue)
        }

        eventLoop.advanceTime(to: Self.deadline(10))
        #expect(Set(log.names) == ["0", "1", "2"])
        #expect(log.fired.allSatisfy { $0.at == Self.deadline(10) })
        #expect(queue._forTesting_tasksScheduled == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func aCancelledSlotDoesNotFire() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let first = Target("first", log: log)
        let second = Target("second", log: log)
        Self.arm(first, at: 10, in: queue)
        Self.arm(second, at: 20, in: queue)

        queue.cancel(try #require(first.slot))
        #expect(!queue.isArmed(try #require(first.slot)))
        #expect(queue._forTesting_armedSlots == 1)
        // A second cancel does nothing.
        queue.cancel(try #require(first.slot))
        #expect(queue._forTesting_armedSlots == 1)

        eventLoop.advanceTime(to: Self.deadline(100))
        #expect(log.names == ["second"])

        // The last cancel stops the task.
        Self.arm(first, at: 200, in: queue)
        #expect(queue._forTesting_isTaskScheduled)
        queue.cancel(try #require(first.slot))
        #expect(!queue._forTesting_isTaskScheduled)
        #expect(queue._forTesting_heapCount == 0)
        eventLoop.advanceTime(to: Self.deadline(300))
        #expect(log.names == ["second"])
    }

    /// A stale slot neither cancels nor moves the deadline of the target its storage went to.
    @available(anyAppleOS 26, *)
    @Test func aStaleSlotTouchesNoOtherTarget() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let first = Target("first", log: log)
        let second = Target("second", log: log)
        Self.arm(first, at: 10, in: queue)
        let stale = try #require(first.slot)
        queue.cancel(stale)
        // The storage `first` had.
        Self.arm(second, at: 20, in: queue)

        queue.cancel(stale)
        #expect(queue.isArmed(try #require(second.slot)))
        let armed = queue.schedule(stale, at: Self.at(5), target: first)
        #expect(armed != stale)

        eventLoop.advanceTime(to: Self.deadline(100))
        #expect(log.fired.map(\.name) == ["first", "second"])
        #expect(log.fired.map(\.at) == [Self.deadline(5), Self.deadline(20)])
    }

    /// Targets run one after the other: a target that arms its own deadline or another's, or
    /// cancels one, while it runs is safe, and a deadline armed then waits for the next task, even
    /// if it is due.
    @available(anyAppleOS 26, *)
    @Test func targetsArmAndCancelWhileTheQueueFires() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let due = (0..<3).map { Target("\($0)", log: log) }
        let later = Target("later", log: log)
        for target in due {
            Self.arm(target, at: 10, in: queue)
        }
        Self.arm(later, at: 50, in: queue)
        // Whichever fires first cancels the next of the others, moves the last earlier, and arms
        // again for now.
        var acted = false
        for target in due {
            target.onFire = {
                guard !acted else { return }
                acted = true
                let others = due.filter { $0 !== target }
                queue.cancel(others[0].slot!)
                Self.arm(others[1], at: 5, in: queue)
                Self.arm(target, at: 10, in: queue)
                Self.arm(later, at: 20, in: queue)
            }
        }

        eventLoop.advanceTime(to: Self.deadline(10))
        let acting = try #require(due.first { $0.name == log.names.first })
        let others = due.filter { $0 !== acting }
        // The moved target's task is due right away; `EmbeddedEventLoop` runs it at its deadline.
        #expect(log.names == [acting.name, others[1].name, acting.name])
        #expect(log.fired.map(\.at) == [Self.deadline(10), Self.deadline(5), Self.deadline(10)])
        eventLoop.advanceTime(to: Self.deadline(100))
        #expect(log.names == [acting.name, others[1].name, acting.name, "later"])
        #expect(log.fired.last?.at == Self.deadline(20))
        #expect(queue._forTesting_armedSlots == 0)
    }

    /// Moving deadlines earlier leaves stale entries, which the queue drops once they dominate.
    @available(anyAppleOS 26, *)
    @Test func staleEntriesAreDropped() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        let targets = (0..<4).map { Target("\($0)", log: log) }
        for step in 0..<1000 {
            for (index, target) in targets.enumerated() {
                Self.arm(target, at: 100_000 - Int64(step * 10 + index), in: queue)
            }
        }

        #expect(queue._forTesting_compactions > 0)
        #expect(queue._forTesting_heapCount <= 64 + targets.count)
        #expect(queue._forTesting_armedSlots == targets.count)
        eventLoop.advanceTime(to: Self.deadline(200_000))
        #expect(log.names == ["3", "2", "1", "0"])
        #expect(log.fired.map(\.at) == (0..<4).reversed().map { Self.deadline(100_000 - Int64(9990 + $0)) })
    }

    /// A slot holds its target from the time it is armed until it is cancelled or fires.
    @available(anyAppleOS 26, *)
    @Test func aSlotHoldsItsTargetWhileArmed() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        var released: Set<String> = []
        var cancelledSlot: StateMachineTimerQueue.Slot?
        do {
            let cancelled = Target("cancelled", log: log)
            let fired = Target("fired", log: log)
            for target in [cancelled, fired] {
                target.onDeinit = { [name = target.name] in released.insert(name) }
                Self.arm(target, at: 10, in: queue)
            }
            cancelledSlot = cancelled.slot
        }
        #expect(released.isEmpty)

        queue.cancel(try #require(cancelledSlot))
        #expect(released == ["cancelled"])
        eventLoop.advanceTime(to: Self.deadline(10))
        #expect(log.names == ["fired"])
        #expect(released == ["cancelled", "fired"])
    }

    /// `cancelAll()` releases every target and cancels the task; the queue works on after it.
    @available(anyAppleOS 26, *)
    @Test func cancelAllReleasesEveryTarget() throws {
        let eventLoop = EmbeddedEventLoop()
        let queue = StateMachineTimerQueue(eventLoop: eventLoop)
        let log = Target.Log(eventLoop: eventLoop)
        var released = 0
        var slots: [StateMachineTimerQueue.Slot] = []
        do {
            for index in 0..<3 {
                let target = Target("\(index)", log: log)
                target.onDeinit = { released += 1 }
                Self.arm(target, at: 10 + Int64(index), in: queue)
                slots.append(try #require(target.slot))
            }
        }

        queue.cancelAll()
        #expect(released == 3)
        #expect(!queue._forTesting_isTaskScheduled)
        #expect(queue._forTesting_armedSlots == 0)

        let target = Target("after", log: log)
        Self.arm(target, at: 20, in: queue)
        // The slots from before stay stale, though their storage is reused.
        for slot in slots {
            queue.cancel(slot)
        }
        eventLoop.advanceTime(to: Self.deadline(100))
        #expect(log.names == ["after"])
    }
}
#endif
