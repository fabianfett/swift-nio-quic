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

/// An opaque handle for a stream on a connection.
public struct QUICStreamHandle: Hashable, Sendable {
    @usableFromInline
    var _index: Index
    @usableFromInline
    var _generation: Generation

    /// The index of the slot the stream occupies in the connection's stream table.
    @inlinable
    var index: Index {
        self._index
    }

    /// How many times that slot has been reused.
    @inlinable
    var generation: Generation {
        self._generation
    }

    /// Creates a new handle from its slot index and generation.
    @inlinable
    init(index: Index, generation: Generation) {
        self._index = index
        self._generation = generation
    }
}

extension QUICStreamHandle {
    @usableFromInline
    struct Generation: Hashable, Sendable {
        @usableFromInline
        var rawValue: UInt32

        @inlinable
        init(_ rawValue: UInt32) {
            self.rawValue = rawValue
        }

        /// The first generation of a slot.
        @inlinable
        static var first: Generation {
            Generation(0)
        }

        @inlinable
        mutating func advance() {
            // Wrapping is fine: it takes a full generation counter's worth of reuses of one slot,
            // by which point any handle old enough _should_ be long gone.
            self.rawValue &+= 1
        }
    }
}

extension QUICStreamHandle {
    /// The index into ``QUICStreamSlots`` for the handle the stream occupies.
    @usableFromInline
    struct Index: Hashable, Comparable, Sendable {
        @usableFromInline
        var _rawValue: UInt32

        /// The slot the stream occupies, as a subscript into storage which is addressed by `Int`.
        @inlinable
        var rawValue: Int { Int(self._rawValue) }

        @inlinable
        init(_ rawValue: Int) {
            self._rawValue = UInt32(rawValue)
        }

        /// The first slot a store hands out.
        @inlinable
        static var first: Index {
            Index(0)
        }

        @inlinable
        mutating func advance() {
            self._rawValue &+= 1
        }

        @inlinable
        static func < (lhs: Index, rhs: Index) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }
}
