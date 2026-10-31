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

/// Groups datagrams written back to back into one buffer into runs, each of which goes to the
/// kernel in one UDP send.
///
/// With UDP Generic Segmentation Offload the kernel splits a run into datagrams of its segment
/// size, the last of which may be shorter. So a run is consecutive datagrams of one size, ended
/// by at most one shorter datagram: a larger datagram, such as a PMTUD probe, or any datagram
/// after a shorter one starts a new run.
@available(anyAppleOS 26, *)
struct DatagramRuns {
    /// A run of datagrams in the buffer.
    struct Run: Hashable {
        /// Where the run starts in the buffer.
        var offset: Int
        /// The bytes of all its datagrams.
        var length: Int
        /// The size of each datagram but the last, which may be shorter.
        var segmentSize: Int
        /// How many datagrams the run has.
        var segments: Int

        /// Whether a datagram of up to the segment size can still join: the last one isn't
        /// shorter.
        var isOpen: Bool {
            self.length == self.segments &* self.segmentSize
        }
    }

    /// The most datagrams one send may carry where the platform has UDP GSO: the kernel's
    /// `UDP_MAX_SEGMENTS`, which is 64 since Linux 4.18 (more on newer kernels). One elsewhere:
    /// every datagram is its own send.
    static var platformMaximumSegments: Int {
        #if os(Linux) || os(Android)
        64
        #else
        1
        #endif
    }

    /// The most bytes one UDP send carries: the IPv4 length field covers the IP and UDP headers,
    /// which leaves 65535 - 20 - 8 bytes of payload (IPv6 allows 65527).
    static var maximumUDPPayload: Int { 65507 }

    /// The most datagrams in a run.
    var maximumSegments: Int
    /// The most bytes in a run.
    let maximumLength: Int

    /// The runs, in the order their datagrams were written.
    private(set) var runs: [Run] = []
    /// The bytes of all runs: where the next datagram starts.
    private(set) var length = 0

    init(
        maximumSegments: Int = Self.platformMaximumSegments,
        maximumLength: Int = Self.maximumUDPPayload
    ) {
        precondition(maximumSegments > 0 && maximumLength > 0)
        self.maximumSegments = maximumSegments
        self.maximumLength = maximumLength
    }

    var isEmpty: Bool {
        self.runs.isEmpty
    }

    /// Adds the datagram of `size` bytes written at ``length``.
    mutating func append(_ size: Int) {
        precondition(size > 0)
        let last = self.runs.count &- 1
        if last >= 0, self.runs[last].isOpen, size <= self.runs[last].segmentSize,
            self.runs[last].segments < self.maximumSegments,
            self.runs[last].length &+ size <= self.maximumLength
        {
            self.runs[last].segments &+= 1
            self.runs[last].length &+= size
        } else {
            self.runs.append(Run(offset: self.length, length: size, segmentSize: size, segments: 1))
        }
        self.length &+= size
    }

    /// Takes the datagram of `size` bytes appended last out of its run again.
    mutating func removeLast(_ size: Int) {
        let last = self.runs.count &- 1
        precondition(last >= 0 && size <= self.runs[last].length)
        if self.runs[last].segments == 1 {
            precondition(size == self.runs[last].length)
            self.runs.removeLast()
        } else {
            self.runs[last].segments &-= 1
            self.runs[last].length &-= size
        }
        self.length &-= size
    }

    /// Forgets all runs, keeping their storage for the next buffer.
    mutating func removeAll() {
        self.runs.removeAll(keepingCapacity: true)
        self.length = 0
    }
}
