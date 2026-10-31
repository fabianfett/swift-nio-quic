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

import Testing

@testable import NIOQUIC

struct DatagramRunsTests {
    /// The runs of datagrams of `sizes`, as (segment size, segments, length).
    @available(anyAppleOS 26, *)
    private func runs(
        _ sizes: [Int],
        maximumSegments: Int = 64,
        maximumLength: Int = DatagramRuns.maximumUDPPayload
    ) -> [Run] {
        var runs = DatagramRuns(maximumSegments: maximumSegments, maximumLength: maximumLength)
        for size in sizes {
            runs.append(size)
        }
        #expect(runs.length == sizes.reduce(0, +))
        self.expectContiguous(runs)
        return runs.runs.map { Run(segmentSize: $0.segmentSize, segments: $0.segments, length: $0.length) }
    }

    private struct Run: Hashable, CustomStringConvertible {
        var segmentSize: Int
        var segments: Int
        var length: Int

        var description: String {
            "\(self.segments)x\(self.segmentSize)(\(self.length))"
        }
    }

    /// Each run starts where the one before ends, and the last ends at the total length.
    @available(anyAppleOS 26, *)
    private func expectContiguous(_ runs: DatagramRuns, sourceLocation: SourceLocation = #_sourceLocation) {
        var offset = 0
        for run in runs.runs {
            #expect(run.offset == offset, sourceLocation: sourceLocation)
            #expect(run.segments > 0, sourceLocation: sourceLocation)
            #expect(run.length > (run.segments - 1) * run.segmentSize, sourceLocation: sourceLocation)
            #expect(run.length <= run.segments * run.segmentSize, sourceLocation: sourceLocation)
            offset += run.length
        }
        #expect(offset == runs.length, sourceLocation: sourceLocation)
    }

    @available(anyAppleOS 26, *)
    @Test
    func noDatagramsMakeNoRuns() {
        let runs = DatagramRuns(maximumSegments: 64)
        #expect(runs.isEmpty)
        #expect(runs.length == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func oneDatagramIsARunOfOne() {
        #expect(self.runs([1200]) == [Run(segmentSize: 1200, segments: 1, length: 1200)])
    }

    @available(anyAppleOS 26, *)
    @Test
    func datagramsOfOneSizeMakeOneRun() {
        #expect(self.runs([1200, 1200, 1200]) == [Run(segmentSize: 1200, segments: 3, length: 3600)])
    }

    @available(anyAppleOS 26, *)
    @Test
    func aShorterDatagramEndsTheRun() {
        #expect(self.runs([1200, 1200, 700]) == [Run(segmentSize: 1200, segments: 3, length: 3100)])
        // The kernel makes every segment but the last the segment size: nothing joins after a
        // shorter one, not even one of the segment size.
        #expect(
            self.runs([1200, 700, 1200, 700, 300]) == [
                Run(segmentSize: 1200, segments: 2, length: 1900),
                Run(segmentSize: 1200, segments: 2, length: 1900),
                Run(segmentSize: 300, segments: 1, length: 300),
            ]
        )
        #expect(
            self.runs([1200, 700, 700]) == [
                Run(segmentSize: 1200, segments: 2, length: 1900),
                Run(segmentSize: 700, segments: 1, length: 700),
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func aLargerDatagramStartsANewRun() {
        // A short datagram first, such as an ACK, then full ones.
        #expect(
            self.runs([50, 1200, 1200]) == [
                Run(segmentSize: 50, segments: 1, length: 50),
                Run(segmentSize: 1200, segments: 2, length: 2400),
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func aPMTUDProbeAndTheLargerDatagramsAfterIt() {
        // A probe larger than the segment size starts a new run; once the path MTU grows,
        // datagrams of the new size make the next one.
        #expect(
            self.runs([1200, 1200, 1500, 1200, 1400, 1400, 1400]) == [
                Run(segmentSize: 1200, segments: 2, length: 2400),
                Run(segmentSize: 1500, segments: 2, length: 2700),
                Run(segmentSize: 1400, segments: 3, length: 4200),
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func aRunHasAtMostTheMaximumSegments() {
        #expect(
            self.runs(Array(repeating: 1200, count: 10), maximumSegments: 4) == [
                Run(segmentSize: 1200, segments: 4, length: 4800),
                Run(segmentSize: 1200, segments: 4, length: 4800),
                Run(segmentSize: 1200, segments: 2, length: 2400),
            ]
        )
        // A shorter last datagram counts too.
        #expect(
            self.runs([100, 100, 100, 50], maximumSegments: 3) == [
                Run(segmentSize: 100, segments: 3, length: 300),
                Run(segmentSize: 50, segments: 1, length: 50),
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func aRunHasAtMostTheMaximumLength() {
        #expect(
            self.runs([1000, 1000, 1000], maximumLength: 2500) == [
                Run(segmentSize: 1000, segments: 2, length: 2000),
                Run(segmentSize: 1000, segments: 1, length: 1000),
            ]
        )
        // A shorter last datagram fits where a full one doesn't.
        #expect(
            self.runs([1000, 1000, 500], maximumLength: 2500) == [
                Run(segmentSize: 1000, segments: 3, length: 2500)
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func theLongestRunsFitOneUDPSend() {
        // 64 segments of 1200 bytes are more than a UDP payload: 54 fit.
        let fullSize = self.runs(Array(repeating: 1200, count: 64))
        #expect(fullSize.map(\.segments) == [54, 10])
        #expect(fullSize.allSatisfy { $0.length <= 65507 })
        // Small segments are capped at 64.
        #expect(self.runs(Array(repeating: 100, count: 65)).map(\.segments) == [64, 1])
        // 44 segments of 1489 bytes would be 65516 bytes, more than IPv4 carries.
        #expect(self.runs(Array(repeating: 1489, count: 44)).map(\.segments) == [43, 1])
    }

    @available(anyAppleOS 26, *)
    @Test
    func withoutSegmentationEveryDatagramIsItsOwnRun() {
        #expect(
            self.runs([1200, 1200, 700], maximumSegments: 1) == [
                Run(segmentSize: 1200, segments: 1, length: 1200),
                Run(segmentSize: 1200, segments: 1, length: 1200),
                Run(segmentSize: 700, segments: 1, length: 700),
            ]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func onlyLinuxSegments() {
        #if os(Linux) || os(Android)
        #expect(DatagramRuns.platformMaximumSegments == 64)
        #else
        #expect(DatagramRuns.platformMaximumSegments == 1)
        #endif
        #expect(DatagramRuns().maximumSegments == DatagramRuns.platformMaximumSegments)
    }

    @available(anyAppleOS 26, *)
    @Test
    func removingTheLastDatagram() {
        var runs = DatagramRuns(maximumSegments: 64)
        runs.append(1200)
        runs.append(1200)
        runs.append(800)
        runs.removeLast(800)
        #expect(runs.length == 2400)
        #expect(runs.runs == [DatagramRuns.Run(offset: 0, length: 2400, segmentSize: 1200, segments: 2)])
        // The run is open again.
        runs.append(1200)
        #expect(runs.runs == [DatagramRuns.Run(offset: 0, length: 3600, segmentSize: 1200, segments: 3)])

        runs.append(1500)
        runs.removeLast(1500)
        #expect(runs.length == 3600)
        #expect(runs.runs == [DatagramRuns.Run(offset: 0, length: 3600, segmentSize: 1200, segments: 3)])

        runs.removeAll()
        #expect(runs.isEmpty)
        #expect(runs.length == 0)
        runs.append(500)
        #expect(runs.runs == [DatagramRuns.Run(offset: 0, length: 500, segmentSize: 500, segments: 1)])
    }
}
