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

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// Holds a connection's event context while NIOQUIC calls out to NIO with it held.
///
/// SwiftNetwork's `NetworkContext.EventContext` is an exclusive token: acquiring it via
/// `fromExternal` while it is already held traps. NIO's pipeline dispatch is synchronous and can't
/// carry the token, so a handler reacting to an event NIOQUIC fired while holding it (writing in
/// `channelRead`, re-reading from `channelReadComplete`, closing from `errorCaught`) re-enters
/// NIOQUIC with no way to reach it.
///
/// So NIOQUIC parks the token here around every call-out (``parking(_:_:)``), and every NIOQUIC
/// entry point acquires it through ``withEventContext(entering:_:)``, which takes the parked token
/// when there is one and only falls back to `fromExternal` when there isn't. The token is moved,
/// never aliased: at any moment it lives in exactly one place.
///
/// There is one slot per `NetworkContext`, i.e. one per connection.
///
/// ## Rules
///
/// Code in NIOQUIC that uses the event context follows these rules:
///
/// - Acquire the event context only through the connection's slot, never with `fromExternal`
///   directly: that traps while the context is held, including while it's parked here.
/// - Enter as the NIOQUIC instance that makes the call, never as a framework instance it calls
///   into: a down-call requires its callee to be idle, and entering as the callee makes it busy.
/// - Park the held context around every call-out that can re-enter NIOQUIC: NIO pipeline events,
///   promise completions and consumer code.
/// - Attach new flows only with nothing held (see ``isHeld`` and ``whenReleased(_:)``): attaching
///   acquires the context itself, as does registering without an event context.
/// - Unregister every registered instance on every end-of-life path: a registered event manager
///   traps when it is released.
@available(anyAppleOS 26, *)
final class EventContextSlot {
    /// The context whose event context this slot parks.
    let context: NetworkContext

    /// The parked event context, or `nil` when nothing up the stack parked one.
    private var parked: NetworkContext.EventContext?

    /// Whether a `withEventContext` call that acquired the context itself (instead of taking a
    /// parked one) is still running.
    private var acquired: Bool

    /// Work queued by `whenReleased(_:)`, run once the acquisition this slot made unwinds.
    private var afterRelease: [() -> Void]

    init(context: NetworkContext) {
        self.context = context
        self.parked = nil
        self.acquired = false
        self.afterRelease = []
    }

    /// Whether this connection's event context is acquired or parked through this slot.
    ///
    /// It can't see a context the framework holds. Inside the nested path of
    /// ``withEventContext(entering:_:)``, which takes the parked context, it is false unless this
    /// slot's own acquisition is still running further up the stack.
    var isHeld: Bool {
        self.acquired || self.parked != nil
    }

    /// Runs `body` with `eventContext` parked, so NIOQUIC entry points that `body` re-enters can
    /// use it.
    ///
    /// Wrap every call into NIO — pipeline events, promise completions, `pipeline.read()` — made
    /// while holding an event context. `eventContext` is moved into the slot for the duration, so
    /// `body` can't use it.
    func parking<Value>(
        _ eventContext: inout NetworkContext.EventContext,
        _ body: () -> Value
    ) -> Value {
        precondition(self.parked == nil, "An event context is already parked")
        self.parked = consume eventContext
        let value = body()
        guard let returned = self.parked.take() else {
            preconditionFailure("A parked event context was not returned to its slot")
        }
        eventContext = returned
        return value
    }

    /// Runs `body` with this connection's event context, entering the stack as `instance`.
    ///
    /// Takes the parked event context if a caller up the stack parked one, otherwise acquires it
    /// via `fromExternal`. NIOQUIC entry points use this instead of calling `fromExternal`
    /// directly.
    func withEventContext<Instance: ProtocolInstance, Value, Failure: Error>(
        entering instance: Instance,
        _ body: (inout NetworkContext.EventContext) throws(Failure) -> Value
    ) throws(Failure) -> Value {
        precondition(instance.context === self.context, "Instance belongs to a different NetworkContext")
        guard var eventContext = self.parked.take() else {
            return try self.acquire(entering: instance, body)
        }
        let result: Result<Value, Failure>
        do throws(Failure) {
            result = .success(try instance.identifier.fromExternal(in: &eventContext, body))
        } catch {
            result = .failure(error)
        }
        self.parked = consume eventContext
        return try result.get()
    }

    /// Acquires the context via `fromExternal`, then runs the work `whenReleased(_:)` queued meanwhile.
    private func acquire<Instance: ProtocolInstance, Value, Failure: Error>(
        entering instance: Instance,
        _ body: (inout NetworkContext.EventContext) throws(Failure) -> Value
    ) throws(Failure) -> Value {
        self.acquired = true
        let result: Result<Value, Failure>
        do throws(Failure) {
            result = .success(try instance.fromExternal(body))
        } catch {
            result = .failure(error)
        }
        self.acquired = false
        self.runAfterRelease()
        return try result.get()
    }

    /// Runs `action` once no event context is held.
    ///
    /// A new flow can't be attached while the context is held: the framework's attach path
    /// acquires the context itself and traps on a held one, parked or not. Code holding the context
    /// queues such work here. It runs as soon as this slot's acquisition unwinds. If the framework
    /// itself holds the context (a timer, say), or nothing holds it, it runs on the next event loop
    /// tick instead.
    func whenReleased(_ action: @escaping () -> Void) {
        if self.acquired {
            self.afterRelease.append(action)
        } else {
            self.context.async(action)
        }
    }

    private func runAfterRelease() {
        // One pass empties the queue. The work runs with `acquired` false, so whatever it queues
        // either goes to the next tick or runs when its own nested acquisition is released.
        let actions = self.afterRelease
        self.afterRelease = []
        for action in actions {
            action()
        }
    }
}

/// A protocol instance that exists only to enter the stack on behalf of code with no instance of its
/// own: the stream table's operations, which act for streams that aren't `ProtocolInstance`s.
@available(anyAppleOS 26, *)
final class EventContextAnchor: ProtocolInstance {
    let context: NetworkContext
    var identifier: InstanceIdentifier
    var eventManager: ProtocolEventManager

    /// Registers with `context`, which acquires the event context: must be called with none held.
    init(context: NetworkContext) {
        self.context = context
        var eventManager = ProtocolEventManager()
        self.identifier = InstanceIdentifier(context: context, eventManager: &eventManager)
        self.eventManager = eventManager
    }

    /// Unregisters, so its event manager doesn't trap on deinit. Idempotent.
    func retire(through slot: EventContextSlot) {
        if self.identifier.isNone { return }
        slot.withEventContext(entering: self) { eventContext in
            self.unregisterEventManager(in: &eventContext)
        }
    }
}
