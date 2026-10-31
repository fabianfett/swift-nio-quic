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
import Synchronization

/// An address the connection sets on its event loop and its channels read from any thread.
@available(anyAppleOS 26, *)
final class SendableAddress: Sendable {
    private let address: Mutex<SocketAddress>

    init(_ address: SocketAddress) {
        self.address = Mutex(address)
    }

    var value: SocketAddress {
        get {
            self.address.withLock { $0 }
        }
        set {
            self.address.withLock { $0 = newValue }
        }
    }
}
