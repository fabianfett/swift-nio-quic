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

/// Writes the datagrams a connection sends in a drain back to back into one buffer, and sends
/// them in the runs of ``DatagramRuns`` once nothing more goes into the buffer.
///
/// An envelope's slice shares the buffer's storage, and writing into a buffer that shares its
/// storage copies all of it, so the runs are only sliced off once the buffer is finished: when
/// it has no room for another datagram, or when the drain ends.
///
/// A drain's first buffer has room for what the drain before wrote, so a connection that sends
/// little allocates little; a drain that fills it continues in one twice as large.
@available(anyAppleOS 26, *)
struct DatagramBatch {
    /// The largest buffer: about one run of full datagrams.
    static var maximumBufferSize: Int { 64 * 1024 }

    private let allocator: ByteBufferAllocator
    /// The buffer of the drain, if it started one. One the drain wrote nothing into stays for the
    /// next drain.
    private var buffer: ByteBuffer?
    private var runs: DatagramRuns
    /// The length of the datagram written last.
    private var lastDatagramLength = 0
    /// The bytes the drain wrote so far.
    private var drainLength = 0
    /// The bytes the last drain that wrote any wrote.
    private var previousDrainLength = 0

    /// The most datagrams sent in one envelope.
    var maximumSegments: Int {
        get { self.runs.maximumSegments }
        set { self.runs.maximumSegments = newValue }
    }

    #if DEBUG
    /// The envelopes sent.
    private(set) var envelopesSent = 0
    /// The datagrams in them.
    private(set) var datagramsSent = 0
    /// The envelopes with more than one datagram, which the kernel segments.
    private(set) var segmentedEnvelopesSent = 0
    /// The datagrams written, in order, if set; slow.
    var onDatagramWritten: ((ByteBuffer) -> Void)?
    #endif

    init(allocator: ByteBufferAllocator, maximumSegments: Int = DatagramRuns.platformMaximumSegments) {
        self.allocator = allocator
        self.runs = DatagramRuns(maximumSegments: maximumSegments)
    }

    /// Writes the next datagram: `body` gets `capacity` bytes and returns the length of the
    /// datagram it wrote there, zero for none.
    ///
    /// If the buffer has no room for `capacity` more bytes, its runs go to `transport` first and
    /// the datagram starts a new buffer.
    mutating func writeDatagram(
        capacity: Int,
        sendingFullBufferTo transport: some QUICTransport,
        remoteAddress: SocketAddress,
        _ body: (inout MutableSpan<UInt8>) -> Int
    ) -> Int {
        if (self.buffer?.writableBytes ?? 0) < capacity {
            self.startBuffer(capacity: capacity, sendingTo: transport, remoteAddress: remoteAddress)
        }
        // '!' is fine: a buffer was just started if there was none.
        let written = self.buffer!.writeWithUnsafeMutableBytes(minimumWritableBytes: capacity) { pointer in
            let room = UnsafeMutableRawBufferPointer(rebasing: pointer[..<capacity])
            var span = MutableSpan(_unsafeElements: room.bindMemory(to: UInt8.self))
            let written = body(&span)
            precondition(written >= 0 && written <= capacity)
            return written
        }
        guard written > 0 else { return 0 }
        self.runs.append(written)
        self.lastDatagramLength = written
        self.drainLength &+= written
        #if DEBUG
        if let onDatagramWritten = self.onDatagramWritten {
            // A copy: a slice would share the storage the next datagram is written into. '!' is
            // fine: the datagram was just written there.
            let bytes = self.buffer!.getBytes(at: self.buffer!.writerIndex &- written, length: written)!
            onDatagramWritten(ByteBuffer(bytes: bytes))
        }
        #endif
        return written
    }

    /// Sends the full buffer, if there is one, and starts one with room for `capacity` bytes.
    private mutating func startBuffer(
        capacity: Int,
        sendingTo transport: some QUICTransport,
        remoteAddress: SocketAddress
    ) {
        let size: Int
        if let full = self.buffer, !self.runs.isEmpty {
            self.buffer = nil
            self.send(full, to: transport, remoteAddress: remoteAddress)
            size = 2 &* full.capacity
        } else {
            // The drain's first buffer, or the one the last drain left was too small: room for
            // what the last drain wrote, and one more datagram.
            size = self.previousDrainLength &+ capacity
        }
        self.buffer = self.allocator.buffer(capacity: max(min(size, Self.maximumBufferSize), capacity))
    }

    /// Ends the drain: sends the runs of its buffer to `transport`.
    ///
    /// - Parameter holdingLast: Whether the datagram written last stays back, for a later
    ///   departure (RFC 9002 Section 7.7).
    /// - Returns: The datagram written last, if `holdingLast`; it shares the buffer's storage.
    mutating func finishDrain(
        sendingTo transport: some QUICTransport,
        remoteAddress: SocketAddress,
        holdingLast: Bool = false
    ) -> ByteBuffer? {
        defer {
            if self.drainLength > 0 {
                self.previousDrainLength = self.drainLength
                self.drainLength = 0
            }
        }
        // A buffer nothing was written into stays for the next drain.
        guard let buffer = self.buffer, !self.runs.isEmpty else {
            precondition(!holdingLast)
            return nil
        }
        self.buffer = nil
        var held: ByteBuffer?
        if holdingLast {
            self.runs.removeLast(self.lastDatagramLength)
            // '!' is fine: the datagram is in the buffer.
            held = buffer.getSlice(at: buffer.readerIndex &+ self.runs.length, length: self.lastDatagramLength)!
        }
        self.send(buffer, to: transport, remoteAddress: remoteAddress)
        return held
    }

    /// Sends the runs of `buffer`, which takes no more writes.
    private mutating func send(
        _ buffer: ByteBuffer,
        to transport: some QUICTransport,
        remoteAddress: SocketAddress
    ) {
        for run in self.runs.runs {
            // '!' is fine: the run is in the buffer.
            let data = buffer.getSlice(at: buffer.readerIndex &+ run.offset, length: run.length)!
            if run.segments > 1 {
                let metadata = AddressedEnvelope<ByteBuffer>.Metadata(
                    ecnState: .transportNotCapable,
                    packetInfo: nil,
                    segmentSize: run.segmentSize
                )
                transport.writeDatagram(
                    AddressedEnvelope(remoteAddress: remoteAddress, data: data, metadata: metadata),
                    promise: nil
                )
            } else {
                transport.writeDatagram(AddressedEnvelope(remoteAddress: remoteAddress, data: data), promise: nil)
            }
            #if DEBUG
            self.envelopesSent += 1
            self.datagramsSent += run.segments
            if run.segments > 1 {
                self.segmentedEnvelopesSent += 1
            }
            #endif
        }
        self.runs.removeAll()
    }
}
