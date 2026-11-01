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

import NIOQUICHelpers

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Opens a stream. The connection assigns its ID once the peer's stream limit allows it.
    ///
    /// - Parameters:
    ///   - type: The type of stream to open.
    ///   - state: The consumer's state for the new stream.
    /// - Returns: The handle for the new stream.
    /// - Throws: If the type is not one this endpoint may initiate, or the table is closed, in
    ///   which case `state` is destroyed.
    func open(_ type: QUICStreamType, state: consuming Consumer.StreamState) throws -> QUICStreamHandle {
        if type.isClientInitiated && self.role == .server {
            throw QUICError.invalidStreamTypeForRole
        }
        if type.isServerInitiated && self.role == .client {
            throw QUICError.invalidStreamTypeForRole
        }

        switch self.backend {
        case .stateMachine(let connection):
            let handle = self.insertSlot(id: nil, state: consume state)
            connection.openStream(for: handle, bidirectional: !type.isUnidirectional)
            return handle
        case .test(let operations):
            let handle = self.insertSlot(id: nil, state: consume state)
            operations.openStream(for: handle, bidirectional: !type.isUnidirectional)
            return handle
        case .closed:
            throw QUICError.invalidStreamState
        }
    }
}
