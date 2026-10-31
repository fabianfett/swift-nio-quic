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

/// Generator for IDs with a numeric value that cannot be accessed from the `ID` type. IDs are backed by
/// a `Counter` that must conform to `FixedWidthInteger` and `Hashable`. The internal ID will
/// overflow without an error.
struct OpaqueIDGenerator<T: FixedWidthInteger & Hashable> {
    private var counter: T = 0

    struct ID: Hashable {
        fileprivate init(value: T) {
            self.value = value
        }

        private let value: T
    }

    /// Generate a new ID.
    mutating func generate() -> ID {
        self.counter &+= 1
        return ID(value: self.counter)
    }
}
