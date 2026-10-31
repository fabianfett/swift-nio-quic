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

/// The handle a QUIC stream uses to talk to the SwiftNetwork transport stack.
@available(anyAppleOS 26, *)
struct SwiftNetworkStreamHandle: ~Copyable {
    /// Pure state machine: tracks attachment.
    ///
    /// As opposed to the other state machines this one specifically
    /// relates to the *SwiftNetwork reference/handle* state.
    struct StateMachine: ~Copyable {
        enum State: ~Copyable {
            case attached
            case detached
        }

        private var state: State

        init() {
            self.state = .attached
        }

        // MARK: - Queries

        var isAttached: Bool {
            switch self.state {
            case .attached: return true
            case .detached: return false
            }
        }

        // MARK: - Per-operation transitions

        enum ViolationReason: CustomStringConvertible {
            /// The handle has been detached; the linkage is gone.
            case detached

            var description: String {
                switch self {
                case .detached: return "linkage detached"
                }
            }
        }

        enum InvokeConnectAction: ~Copyable {
            case performConnect
            case handleViolation(ViolationReason)
        }
        func invokeConnect() -> InvokeConnectAction {
            switch self.state {
            case .attached: return .performConnect
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeDisconnectAction: ~Copyable {
            case performDisconnect
            case ignore
        }
        func invokeDisconnect() -> InvokeDisconnectAction {
            switch self.state {
            case .attached: return .performDisconnect
            case .detached: return .ignore
            }
        }

        enum InvokeAbortInboundAction: ~Copyable {
            case performAbortInbound
            case ignore
        }
        func invokeAbortInbound() -> InvokeAbortInboundAction {
            switch self.state {
            case .attached: return .performAbortInbound
            case .detached: return .ignore
            }
        }

        enum InvokeAbortOutboundAction: ~Copyable {
            case performAbortOutbound
            case ignore
        }
        func invokeAbortOutbound() -> InvokeAbortOutboundAction {
            switch self.state {
            case .attached: return .performAbortOutbound
            case .detached: return .ignore
            }
        }

        enum InvokeSendStreamDataAction: ~Copyable {
            case performSendStreamData
            case handleViolation(ViolationReason)
        }
        func invokeSendStreamData() -> InvokeSendStreamDataAction {
            switch self.state {
            case .attached: return .performSendStreamData
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeReceiveStreamDataAction: ~Copyable {
            case performReceiveStreamData
            case handleViolation(ViolationReason)
        }
        func invokeReceiveStreamData() -> InvokeReceiveStreamDataAction {
            switch self.state {
            case .attached: return .performReceiveStreamData
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeGetMetadataAction: ~Copyable {
            case performGetMetadata
            case ignore
        }
        func invokeGetMetadata() -> InvokeGetMetadataAction {
            switch self.state {
            case .attached: return .performGetMetadata
            case .detached: return .ignore
            }
        }

        // MARK: - Detach (mutating; transitions `.attached` → `.detached`)

        enum InvokeDetachAction: ~Copyable {
            case performDetach
            case skipAlreadyDetached
        }
        mutating func invokeDetach() -> InvokeDetachAction {
            self.state.invokeDetach()
        }
    }

    private var stateMachine: StateMachine
    private var linkage: BaseOutboundStreamLinkage

    /// Initialise with a stub linkage (the outbound starting state, before
    /// `invokeConnect` wires up a real identifier).
    init() {
        self.stateMachine = StateMachine()
        self.linkage = BaseOutboundStreamLinkage()
    }

    /// Initialise with a specific linkage, once the stream's flow is attached: the one
    /// `invokeAttachUpperProtocolToExistingFlow` returns for an inbound stream, or the one the
    /// stack hands an outbound stream's `invokeAttachLowerProtocol`.
    init(linkage: BaseOutboundStreamLinkage) {
        self.stateMachine = StateMachine()
        self.linkage = linkage
    }

    /// Returns `true` while the linkage is still attached.
    var isAttached: Bool {
        self.stateMachine.isAttached
    }

    // MARK: - Wrapper methods
    //
    // Every wrapper here returns an action that, on the success path, carries
    // a *value snapshot* of `self.linkage` (`BaseOutboundStreamLinkage` is a
    // `struct`). Callers extract the linkage from the action, which releases the borrow on
    // `self.swiftNetworkStreamHandle`, and then perform the SwiftNetwork side
    // effect on the local value. This is load-bearing: SwiftNetwork's
    // `handleCallFromUpperProtocol` drains queued peer events in a `defer`
    // after `body()`, so a peer disconnect can synchronously re-enter
    // `handleDisconnectedEvent` from inside an outer `invoke*` call. If the
    // outer call held a `read` access on `self.swiftNetworkStreamHandle` for
    // the duration of its linkage call, the inner `invokeDetach` (a `modify`)
    // would overlap and trap on Swift exclusivity. Releasing the borrow before
    // the side effect makes the re-entry safe at the language level.

    enum InvokeConnectAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeConnect() -> InvokeConnectAction {
        switch self.stateMachine.invokeConnect() {
        case .performConnect:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeDisconnectAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case ignore
    }

    func invokeDisconnect() -> InvokeDisconnectAction {
        switch self.stateMachine.invokeDisconnect() {
        case .performDisconnect:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeAbortInboundAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case ignore
    }

    func invokeAbortInbound() -> InvokeAbortInboundAction {
        switch self.stateMachine.invokeAbortInbound() {
        case .performAbortInbound:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeAbortOutboundAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case ignore
    }

    func invokeAbortOutbound() -> InvokeAbortOutboundAction {
        switch self.stateMachine.invokeAbortOutbound() {
        case .performAbortOutbound:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeSendStreamDataAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeSendStreamData() -> InvokeSendStreamDataAction {
        switch self.stateMachine.invokeSendStreamData() {
        case .performSendStreamData:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeReceiveStreamDataAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeReceiveStreamData() -> InvokeReceiveStreamDataAction {
        switch self.stateMachine.invokeReceiveStreamData() {
        case .performReceiveStreamData:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeGetMetadataAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case ignore
    }

    func invokeGetMetadata() -> InvokeGetMetadataAction {
        switch self.stateMachine.invokeGetMetadata() {
        case .performGetMetadata:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeDetachAction: ~Copyable {
        case proceed(BaseOutboundStreamLinkage)
        case skipAlreadyDetached
    }

    /// Mutating: transitions the SM to `.detached` before returning. The
    /// modify borrow ends with the return; the caller performs `invokeDetach`
    /// on the returned linkage value without holding any borrow on the handle.
    mutating func invokeDetach() -> InvokeDetachAction {
        switch self.stateMachine.invokeDetach() {
        case .performDetach:
            return .proceed(self.linkage)
        case .skipAlreadyDetached:
            return .skipAlreadyDetached
        }
    }

    /// Builds the error raised by soft-violation operations
    /// (`invokeSendStreamData`, `invokeReceiveStreamData`). The reason is
    /// included in the description so callers and logs see exactly what
    /// went wrong.
    static func violationError(
        operation: String,
        reason: StateMachine.ViolationReason
    ) -> NetworkError {
        NetworkError(
            category: NetworkError.CommonCategory(
                identifier: "swift-nio-quic.streamHandleViolation",
                description: "\(operation): \(reason)"
            )
        )
    }
}

// MARK: - SwiftNetwork Stream Handle State Transitions

@available(anyAppleOS 26, *)
extension SwiftNetworkStreamHandle.StateMachine.State {
    mutating func invokeDetach() -> SwiftNetworkStreamHandle.StateMachine.InvokeDetachAction {
        switch consume self {
        case .attached:
            self = .detached
            return .performDetach
        case .detached:
            self = .detached
            return .skipAlreadyDetached
        }
    }
}
