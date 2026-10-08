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
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

struct EventContextSlotTests {
    /// A registered protocol instance to enter the stack as.
    @available(anyAppleOS 26, *)
    private final class TestInstance: ProtocolInstance {
        let context: NetworkContext
        var identifier: InstanceIdentifier
        var eventManager: ProtocolEventManager

        init(context: NetworkContext) {
            self.context = context
            var eventManager = ProtocolEventManager()
            self.identifier = InstanceIdentifier(context: context, eventManager: &eventManager)
            self.eventManager = eventManager
        }
    }

    private struct Boom: Error {}

    /// Runs `body` with a slot and a registered instance on an event-loop-backed context, and
    /// unregisters the instance afterwards (its event manager traps on deinit otherwise).
    @available(anyAppleOS 26, *)
    private func withSlot(_ body: (EventContextSlot, TestInstance) -> Void) {
        let loop = EmbeddedEventLoop()
        let context = NetworkContext(
            identifier: "EventContextSlotTests",
            externalScheduler: EventLoopBackedScheduler(eventLoop: loop)
        )
        let slot = EventContextSlot(context: context)
        let instance = TestInstance(context: context)
        body(slot, instance)
        slot.withEventContext(entering: instance) { eventContext in
            instance.unregisterEventManager(in: &eventContext)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func acquiresTheContextWhenNothingIsParked() {
        self.withSlot { slot, instance in
            let value = slot.withEventContext(entering: instance) { _ in 42 }
            #expect(value == 42)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func reentryWhileParkedUsesTheParkedContext() {
        // Without the slot this inner acquisition is an exclusivity violation and traps.
        self.withSlot { slot, instance in
            let value = slot.withEventContext(entering: instance) { eventContext in
                slot.parking(&eventContext) {
                    slot.withEventContext(entering: instance) { _ in "nested" }
                }
            }
            #expect(value == "nested")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func parkingNestsThroughAReentrantCallback() {
        // Mirrors NIO entry -> call-out -> re-entry -> stack call -> the stack calls a witness,
        // which parks again around its own call-out -> re-entry.
        func witness(
            in eventContext: inout NetworkContext.EventContext,
            _ slot: EventContextSlot,
            _ instance: TestInstance
        ) -> Int {
            slot.parking(&eventContext) {
                slot.withEventContext(entering: instance) { _ in 3 }
            }
        }

        self.withSlot { slot, instance in
            let value = slot.withEventContext(entering: instance) { eventContext in
                slot.parking(&eventContext) {
                    slot.withEventContext(entering: instance) { eventContext in
                        witness(in: &eventContext, slot, instance)
                    }
                }
            }
            #expect(value == 3)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aThrowingReentryReturnsTheContextToTheSlot() {
        self.withSlot { slot, instance in
            slot.withEventContext(entering: instance) { eventContext in
                slot.parking(&eventContext) {
                    #expect(throws: Boom.self) {
                        try slot.withEventContext(entering: instance) { (_) throws(Boom) -> Void in
                            throw Boom()
                        }
                    }
                    // The failed re-entry must have put the context back; a second one still finds it.
                    let value = slot.withEventContext(entering: instance) { _ in "still parked" }
                    #expect(value == "still parked")
                }
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func reentryAsAnotherInstanceUsesTheParkedContext() {
        // The common production case: the instance that re-enters differs from the holder, so the
        // nested entry starts and finishes its own external call.
        self.withSlot { slot, holder in
            let other = TestInstance(context: slot.context)
            let value = slot.withEventContext(entering: holder) { eventContext in
                slot.parking(&eventContext) {
                    slot.withEventContext(entering: other) { _ in "other" }
                }
            }
            #expect(value == "other")
            slot.withEventContext(entering: other) { eventContext in
                other.unregisterEventManager(in: &eventContext)
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func workQueuedWhileAcquiredRunsOnceReleased() {
        self.withSlot { slot, instance in
            var ran = false
            slot.withEventContext(entering: instance) { _ in
                slot.whenReleased { ran = true }
                #expect(!ran)
            }
            #expect(ran)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func workQueuedFromAParkedCallOutMayAcquireOnceReleased() {
        // A stream opened from a handler: queued inside a parked call-out, and it acquires the
        // context again itself, which is only legal once nothing holds it.
        self.withSlot { slot, instance in
            var acquiredAgain = false
            slot.withEventContext(entering: instance) { eventContext in
                slot.parking(&eventContext) {
                    #expect(slot.isHeld)
                    slot.whenReleased {
                        #expect(!slot.isHeld)
                        slot.withEventContext(entering: instance) { _ in acquiredAgain = true }
                    }
                }
            }
            #expect(acquiredAgain)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func workQueuedWithoutAnAcquisitionRunsOnTheNextTick() {
        // Here the framework, not NIOQUIC, would hold the context (a timer, say), so the slot
        // can't tell when it is released and defers to the event loop.
        let loop = EmbeddedEventLoop()
        let context = NetworkContext(
            identifier: "EventContextSlotTests",
            externalScheduler: EventLoopBackedScheduler(eventLoop: loop)
        )
        let slot = EventContextSlot(context: context)
        var ran = false
        slot.whenReleased { ran = true }
        #expect(!ran)
        loop.run()
        #expect(ran)
    }

    @available(anyAppleOS 26, *)
    @Test
    func aThrowingAcquisitionReleasesAndRunsQueuedWork() {
        self.withSlot { slot, instance in
            var ran = false
            #expect(throws: Boom.self) {
                try slot.withEventContext(entering: instance) { (_) throws(Boom) -> Void in
                    slot.whenReleased { ran = true }
                    throw Boom()
                }
            }
            #expect(!slot.isHeld)
            #expect(ran)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func workQueuedInsideQueuedWorksAcquisitionRunsWhenThatUnwinds() {
        self.withSlot { slot, instance in
            var ran = false
            slot.withEventContext(entering: instance) { _ in
                slot.whenReleased {
                    slot.withEventContext(entering: instance) { _ in
                        slot.whenReleased { ran = true }
                        #expect(!ran)
                    }
                    #expect(ran)
                }
            }
            #expect(ran)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aRetiredAnchorLeavesNothingRegistered() {
        // Its event manager traps on deinit if still registered, so completing is the check.
        self.withSlot { slot, _ in
            let anchor = EventContextAnchor(context: slot.context)
            #expect(!anchor.identifier.isNone)
            anchor.retire(through: slot)
            #expect(anchor.identifier.isNone)
            anchor.retire(through: slot)
            #expect(anchor.identifier.isNone)
        }
    }
}
