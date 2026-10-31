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
import Testing

@testable import NIOQUIC

struct DatagramBatchTests {
    private static let peer = try! SocketAddress(ipAddress: "127.0.0.1", port: 4433)

    /// The bytes of the `index`th datagram written.
    private static func datagram(_ index: Int, size: Int) -> [UInt8] {
        (0..<size).map { UInt8(truncatingIfNeeded: index &* 31 &+ $0) }
    }

    /// Writes datagram `index` of `size` bytes, with room for `capacity`; returns where it went.
    @available(anyAppleOS 26, *)
    @discardableResult
    private func write(
        _ batch: inout DatagramBatch,
        _ index: Int,
        size: Int,
        capacity: Int = 1500,
        to transport: RecordingTransport,
        sourceLocation: SourceLocation = #_sourceLocation
    ) -> UInt {
        var address: UInt = 0
        let bytes = Self.datagram(index, size: size)
        let written = batch.writeDatagram(capacity: capacity, sendingFullBufferTo: transport, remoteAddress: Self.peer)
        { span in
            #expect(span.count == capacity, sourceLocation: sourceLocation)
            for (offset, byte) in bytes.enumerated() {
                span[offset] = byte
            }
            span.withUnsafeMutableBufferPointer { address = UInt(bitPattern: $0.baseAddress) }
            return size
        }
        #expect(written == size, sourceLocation: sourceLocation)
        return address
    }

    private struct Sent: Hashable {
        var bytes: [UInt8]
        var segmentSize: Int?
        /// Where its bytes are.
        var address: UInt
    }

    /// The envelopes written to `transport`, which it forgets.
    @available(anyAppleOS 26, *)
    private func sent(_ transport: RecordingTransport) -> [Sent] {
        var sent: [Sent] = []
        while let event = transport.events.popFirst() {
            guard case .wrote(let envelope) = event else {
                Issue.record("unexpected \(event)")
                continue
            }
            #expect(envelope.remoteAddress == Self.peer)
            sent.append(self.sent(envelope.data, segmentSize: envelope.metadata?.segmentSize))
        }
        return sent
    }

    private func sent(_ data: ByteBuffer, segmentSize: Int?) -> Sent {
        Sent(
            bytes: Array(data.readableBytesView),
            segmentSize: segmentSize,
            address: data.withUnsafeReadableBytes { UInt(bitPattern: $0.baseAddress) }
        )
    }

    /// Each of `sent` starts where the one before ends: they are in one buffer, never copied.
    private func expectOneBuffer(_ sent: [Sent], sourceLocation: SourceLocation = #_sourceLocation) {
        for (before, after) in zip(sent, sent.dropFirst()) {
            #expect(after.address == before.address + UInt(before.bytes.count), sourceLocation: sourceLocation)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func theDatagramsOfADrainShareOneBuffer() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 1)
        for index in 0..<3 {
            self.write(&batch, index, size: 1200, to: transport)
        }
        // A drain's first buffer has room for one datagram, so the second and third went into a
        // new one twice as large.
        let firstBuffers = self.sent(transport)
        #expect(firstBuffers.map(\.bytes) == [Self.datagram(0, size: 1200)])
        #expect(batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer) == nil)
        let sent = firstBuffers + self.sent(transport)
        #expect(sent.map(\.bytes) == (0..<3).map { Self.datagram($0, size: 1200) })
        #expect(sent.map(\.segmentSize) == [nil, nil, nil])
        self.expectOneBuffer(Array(sent.dropFirst()))

        // The next drain starts with a buffer as large as what the last one wrote.
        for index in 3..<7 {
            self.write(&batch, index, size: [1200, 1200, 1200, 100][index - 3], to: transport)
        }
        #expect(transport.events.isEmpty)
        #expect(batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer) == nil)
        let next = self.sent(transport)
        #expect(next.map(\.bytes.count) == [1200, 1200, 1200, 100])
        #expect(next.map(\.bytes) == (3..<7).map { Self.datagram($0, size: [1200, 1200, 1200, 100][$0 - 3]) })
        self.expectOneBuffer(next)
    }

    @available(anyAppleOS 26, *)
    @Test
    func aFullBufferIsSentBeforeTheNextDatagram() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        self.write(&batch, 0, size: 1200, capacity: 1200, to: transport)
        #expect(transport.events.isEmpty)
        // The buffer has no room for a datagram as large as the capacity.
        let written = batch.writeDatagram(capacity: 1200, sendingFullBufferTo: transport, remoteAddress: Self.peer) {
            _ in
            #expect(transport.events.count == 1)
            return 0
        }
        #expect(written == 0)
        #expect(self.sent(transport).map(\.bytes) == [Self.datagram(0, size: 1200)])
        #expect(batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer) == nil)
        #expect(transport.events.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func aBufferHasRoomForTheCapacityTheCoreAsksFor() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        self.write(&batch, 0, size: 1200, capacity: 1200, to: transport)
        self.write(&batch, 1, size: 1200, capacity: 1200, to: transport)
        self.write(&batch, 2, size: 1200, capacity: 1200, to: transport)
        // A PMTUD probe asks for more than the buffer has left.
        self.write(&batch, 3, size: 9000, capacity: 9000, to: transport)
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        let sent = self.sent(transport)
        #expect(sent.map(\.bytes.count) == [1200, 2400, 9000])
        #expect(sent.map(\.segmentSize) == [nil, 1200, nil])
    }

    @available(anyAppleOS 26, *)
    @Test
    func runsGoOutAsSegmentedEnvelopes() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        // The first drain sizes the buffers of the next.
        for index in 0..<8 {
            self.write(&batch, index, size: 1200, to: transport)
        }
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        _ = self.sent(transport)

        let sizes = [1200, 1200, 700, 1200, 1300, 1300]
        for (index, size) in sizes.enumerated() {
            self.write(&batch, index, size: size, to: transport)
        }
        #expect(transport.events.isEmpty)
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        let sent = self.sent(transport)
        // A run of one has no segment size: NIO only takes one on Linux.
        #expect(sent.map(\.segmentSize) == [1200, nil, 1300])
        #expect(
            sent.map(\.bytes) == [
                Self.datagram(0, size: 1200) + Self.datagram(1, size: 1200) + Self.datagram(2, size: 700),
                Self.datagram(3, size: 1200),
                Self.datagram(4, size: 1300) + Self.datagram(5, size: 1300),
            ]
        )
        self.expectOneBuffer(sent)
    }

    @available(anyAppleOS 26, *)
    @Test
    func theDatagramHeldForLaterIsTheLastOne() throws {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        for index in 0..<8 {
            self.write(&batch, index, size: 1200, to: transport)
        }
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        _ = self.sent(transport)

        for index in 0..<3 {
            self.write(&batch, index, size: 1200, to: transport)
        }
        let heldDatagram = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer, holdingLast: true)
        let held = try #require(heldDatagram)
        let sent = self.sent(transport)
        #expect(sent.map(\.bytes) == [Self.datagram(0, size: 1200) + Self.datagram(1, size: 1200)])
        #expect(sent.map(\.segmentSize) == [1200])
        let heldSent = self.sent(held, segmentSize: nil)
        #expect(heldSent.bytes == Self.datagram(2, size: 1200))
        self.expectOneBuffer(sent + [heldSent])

        // A drain's only datagram can be held too.
        self.write(&batch, 9, size: 1000, to: transport)
        let aloneDatagram = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer, holdingLast: true)
        let alone = try #require(aloneDatagram)
        #expect(transport.events.isEmpty)
        #expect(Array(alone.readableBytesView) == Self.datagram(9, size: 1000))
        // The next drain writes into a new buffer.
        self.write(&batch, 10, size: 1000, to: transport)
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        #expect(self.sent(transport).map(\.bytes) == [Self.datagram(10, size: 1000)])
        #expect(Array(alone.readableBytesView) == Self.datagram(9, size: 1000))
    }

    @available(anyAppleOS 26, *)
    @Test
    func aDrainThatWritesNothingLeavesItsBufferForTheNext() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        var unused: UInt = 0
        let written = batch.writeDatagram(capacity: 1500, sendingFullBufferTo: transport, remoteAddress: Self.peer) {
            span in
            span.withUnsafeMutableBufferPointer { unused = UInt(bitPattern: $0.baseAddress) }
            return 0
        }
        #expect(written == 0)
        #expect(batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer) == nil)
        #expect(transport.events.isEmpty)
        let address = self.write(&batch, 0, size: 1200, to: transport)
        #expect(address == unused)
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        #expect(self.sent(transport).map(\.bytes) == [Self.datagram(0, size: 1200)])
    }

    #if DEBUG
    @available(anyAppleOS 26, *)
    @Test
    func countsWhatItSends() {
        let transport = RecordingTransport()
        var batch = DatagramBatch(allocator: ByteBufferAllocator(), maximumSegments: 64)
        var written: [ByteBuffer] = []
        batch.onDatagramWritten = { written.append($0) }
        for index in 0..<8 {
            self.write(&batch, index, size: 1200, to: transport)
        }
        self.write(&batch, 8, size: 100, to: transport)
        self.write(&batch, 9, size: 100, to: transport)
        _ = batch.finishDrain(sendingTo: transport, remoteAddress: Self.peer)
        let sent = self.sent(transport)
        #expect(batch.datagramsSent == 10)
        #expect(batch.envelopesSent == sent.count)
        #expect(batch.segmentedEnvelopesSent == sent.filter { $0.segmentSize != nil }.count)
        #expect(
            written.map { Array($0.readableBytesView) } == (0..<10).map { Self.datagram($0, size: $0 < 8 ? 1200 : 100) }
        )
    }
    #endif
}
