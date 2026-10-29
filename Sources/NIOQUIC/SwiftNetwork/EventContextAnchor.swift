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

/// A protocol instance that exists only to enter the stack on behalf of code with no instance of its
/// own: the stream table's operations, which act for streams that aren't `ProtocolInstance`s.
///
/// A value, so that its owner holds it inline and entering the stack doesn't load another object.
@available(anyAppleOS 26, *)
struct EventContextAnchor: ProtocolInstance, ~Copyable {
    let context: NetworkContext
    var identifier: InstanceIdentifier
    var eventManager: ProtocolEventManager

    /// Registers with `context`, which acquires the event context: must be called with none held.
    init(context: NetworkContext) {
        self.context = context
        self.eventManager = ProtocolEventManager()
        self.identifier = InstanceIdentifier(context: context, eventManager: &self.eventManager)
    }

    /// An unregistered instance that enters the stack as `identifier`.
    private init(context: NetworkContext, enteringAs identifier: InstanceIdentifier) {
        self.context = context
        self.identifier = identifier
        self.eventManager = ProtocolEventManager()
    }

    /// Unregisters, so its event manager doesn't trap on deinit. Idempotent.
    mutating func retire(through callouts: QUICCallouts) {
        if self.identifier.isNone { return }
        // Enters as a stand-in with the same identifier: entering borrows the instance it enters as
        // for the whole call, and this one changes inside it.
        let entry = EventContextAnchor(context: self.context, enteringAs: self.identifier)
        callouts.enter(as: entry) { eventContext in
            self.unregisterEventManager(in: &eventContext)
        }
    }
}
