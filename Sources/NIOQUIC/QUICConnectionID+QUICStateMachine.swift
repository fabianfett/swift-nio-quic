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

@_spi(Essentials) @_spi(ProtocolProvider) import QUICStateMachine

@available(anyAppleOS 26, *)
extension NIOQUIC.QUICConnectionID {
    /// Creates a NIOQUIC connection ID from a QUICStateMachine connection ID.
    init(_ connectionID: QUICStateMachine.QUICConnectionID) {
        self.init(
            bytes: connectionID.connectionIDStorage,
            length: UInt8(connectionID.length)
        )
    }
}

@available(anyAppleOS 26, *)
extension QUICStateMachine.QUICConnectionID {
    /// Creates a QUICStateMachine connection ID from a NIOQUIC connection ID.
    init(_ connectionID: NIOQUIC.QUICConnectionID) {
        self.init(
            storage: connectionID.bytes,
            size: connectionID.length
        )
    }
}
