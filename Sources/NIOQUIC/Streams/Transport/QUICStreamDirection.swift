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

/// The direction of a QUIC stream.
enum QUICStreamDirection: Hashable {
    /// Bidirectional stream (both send and receive).
    case bidirectional
    /// Unidirectional stream that we can only send on.
    case sendOnly
    /// Unidirectional stream that we can only receive on.
    case receiveOnly
}
