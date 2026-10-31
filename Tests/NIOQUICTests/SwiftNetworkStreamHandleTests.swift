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
import Testing

@testable import NIOQUIC

// MARK: - SwiftNetworkStreamHandle.StateMachine

struct SwiftNetworkStreamHandleStateMachineTests {
    /// Hard violation: starting a torn-down stream is a programmer bug. The wrapper
    /// `preconditionFailure`s on this — which we can't test directly — so we verify
    /// the SM signals it via the action enum here, with the reason that explains why.
    @available(anyAppleOS 26, *)
    @Test("invokeConnect returns .handleViolation(.detached) when detached")
    func invokeConnectReturnsHandleViolation() {
        var sm = SwiftNetworkStreamHandle.StateMachine()

        _ = sm.invokeDetach()

        let action = sm.invokeConnect()
        guard case .handleViolation(.detached) = action else {
            Issue.record("Expected .handleViolation(.detached)")
            return
        }
    }
}

// MARK: - SwiftNetworkStreamHandle (wrapper)

struct SwiftNetworkStreamHandleTests {
    /// Outbound streams start with a stub linkage; the handle is usable from creation.
    @available(anyAppleOS 26, *)
    @Test("Default init is attached")
    func defaultInitIsAttached() {
        let handle = SwiftNetworkStreamHandle()

        let isAttached = handle.isAttached
        #expect(isAttached)
    }

    /// Inbound init pathway: the listener-attached linkage is wrapped in an attached handle.
    @available(anyAppleOS 26, *)
    @Test("init(linkage:) is attached")
    func initWithLinkageIsAttached() {
        let handle = SwiftNetworkStreamHandle(linkage: BaseOutboundStreamLinkage())

        let isAttached = handle.isAttached
        #expect(isAttached)
    }

    /// End-to-end detach contract: wrapper consumes the linkage on the first call and
    /// tolerates duplicates (multiple call sites unconditionally try to tear down).
    @available(anyAppleOS 26, *)
    @Test("invokeDetach flips state and is idempotent")
    func invokeDetachIsIdempotent() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        let isAttachedAfter = handle.isAttached
        #expect(!isAttachedAfter)

        // Second call should report already detached, not crash.
        switch handle.invokeDetach() {
        case .proceed: Issue.record("Expected .skipAlreadyDetached on second detach")
        case .skipAlreadyDetached: break
        }
    }

    /// Stop sequencing: disconnect after detach is a normal teardown ordering and must not throw.
    @available(anyAppleOS 26, *)
    @Test("invokeDisconnect silently no-ops when detached")
    func invokeDisconnectNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeDisconnect() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Error/abort during teardown must not raise (called from the close-stream paths).
    @available(anyAppleOS 26, *)
    @Test("invokeAbortInbound silently no-ops when detached")
    func invokeAbortInboundNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeAbortInbound() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Error/abort during teardown must not raise (called from the close-stream paths).
    @available(anyAppleOS 26, *)
    @Test("invokeAbortOutbound silently no-ops when detached")
    func invokeAbortOutboundNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeAbortOutbound() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Soft-violation policy: write-after-detach surfaces as a violation the caller can
    /// surface as a throw (e.g. return false), not a silent no-op that drops bytes.
    @available(anyAppleOS 26, *)
    @Test("invokeSendStreamData reports violation when detached")
    func invokeSendStreamDataThrowsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeSendStreamData() {
        case .proceed:
            Issue.record("Expected .handleViolation when detached")
        case .handleViolation(let reason):
            switch reason {
            case .detached: break
            }
        }
    }

    /// Soft-violation policy: read-after-detach surfaces as a violation, not a silent nil that
    /// the caller might confuse with "no data available".
    @available(anyAppleOS 26, *)
    @Test("invokeReceiveStreamData reports violation when detached")
    func invokeReceiveStreamDataThrowsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeReceiveStreamData() {
        case .proceed:
            Issue.record("Expected .handleViolation when detached")
        case .handleViolation(let reason):
            switch reason {
            case .detached: break
            }
        }
    }

    /// Metadata is genuinely unavailable post-detach; the wrapper returns `.ignore`
    /// so the caller can surface nil to its own consumer.
    @available(anyAppleOS 26, *)
    @Test("invokeGetMetadata reports ignore when detached")
    func invokeGetMetadataReturnsNilWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeGetMetadata() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }
}
