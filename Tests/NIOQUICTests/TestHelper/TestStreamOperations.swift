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
import NIOQUICHelpers

@testable import NIOQUIC

/// Stream operations without a connection behind them, which behave like a `QUICConnectionCore`'s:
/// streams opened get IDs up to a stream limit and queue beyond it, reads hand over all contiguous
/// bytes at once, and writes, FINs, resets and STOP\_SENDINGs are recorded.
///
/// Holds its table strongly; `QUICStreamTable.releaseBackend()` breaks the cycle.
@available(anyAppleOS 26, *)
final class TestStreamOperations<Consumer: QUICStreamConsumer & ~Copyable>: QUICStreamOperations {
    /// What the peer sent on a stream, and what was sent to it.
    struct Stream {
        /// Bytes received and not yet read.
        var inbound: [UInt8] = []
        /// Whether the peer's FIN arrived after `inbound`.
        var inboundFinished = false
        /// The bytes written, in order.
        var outbound: [UInt8] = []
        /// Whether the send side was finished.
        var finished = false
        /// The code of the RESET\_STREAM sent, if any.
        var resetCode: QUICApplicationErrorCode?
        /// The code of the STOP\_SENDING sent, if any.
        var stopSendingCode: QUICApplicationErrorCode?
        /// How many times the stream was read.
        var reads = 0
    }

    /// The table the operations report opened streams to.
    var table: QUICStreamTable<Consumer>?

    let role: Role

    /// The streams, by ID.
    private(set) var streams: [QUICStreamID: Stream] = [:]

    /// The bidirectional and unidirectional streams the peer allows this endpoint to open.
    var bidirectionalLimit: Int
    var unidirectionalLimit: Int
    private var openedBidirectional = 0
    private var openedUnidirectional = 0

    /// Streams waiting for the peer to allow more.
    private(set) var pendingOpens: [(handle: QUICStreamHandle, bidirectional: Bool)] = []

    /// What ``writableBytes(_:)`` reports for every stream.
    var writableBytesPerStream = 1 << 20

    /// Whether writes are refused, as when the connection closes.
    var refusesWrites = false

    init(role: Role, bidirectionalLimit: Int = 100, unidirectionalLimit: Int = 100) {
        self.role = role
        self.bidirectionalLimit = bidirectionalLimit
        self.unidirectionalLimit = unidirectionalLimit
    }

    /// Makes a table on these operations.
    static func makeTable(
        role: Role,
        bidirectionalLimit: Int = 100,
        unidirectionalLimit: Int = 100
    ) -> (QUICStreamTable<Consumer>, TestStreamOperations) {
        let operations = TestStreamOperations(
            role: role,
            bidirectionalLimit: bidirectionalLimit,
            unidirectionalLimit: unidirectionalLimit
        )
        let table = QUICStreamTable<Consumer>(role: role, operations: operations)
        operations.table = table
        return (table, operations)
    }

    subscript(id: QUICStreamID) -> Stream? {
        self.streams[id]
    }

    // MARK: The peer

    /// The peer sent `bytes`, and its FIN with `fin`, on stream `id`; a stream it opened is
    /// inserted into the table, as a connection does.
    func receive(_ bytes: String, fin: Bool = false, on id: QUICStreamID) {
        if self.streams[id] == nil {
            self.streams[id] = Stream()
            if let table = self.table, table.handle(forID: id) == nil {
                let handle = table.insertSlot(id: id, state: nil)
                table.assignID(id, to: handle)
                table.markReady(handle: handle, events: .opened)
            }
        }
        self.streams[id]!.inbound.append(contentsOf: Array(bytes.utf8))
        if fin {
            self.streams[id]!.inboundFinished = true
        }
        if let table = self.table, let handle = table.handle(forID: id) {
            table.markReady(handle: handle, events: .readable)
        }
    }

    /// The peer sent RESET\_STREAM on stream `id`: what wasn't read is dropped, as a connection
    /// does (RFC 9000 Section 3.2).
    func peerResets(_ id: QUICStreamID, code: QUICApplicationErrorCode) {
        self.streams[id]?.inbound = []
        if let table = self.table, let handle = table.handle(forID: id) {
            table.peerResetStream(handle, code: code)
        }
    }

    /// The peer sent STOP\_SENDING on stream `id`, which a connection answers with RESET\_STREAM.
    func peerStopsSending(_ id: QUICStreamID, code: QUICApplicationErrorCode) {
        if let table = self.table, let handle = table.handle(forID: id) {
            table.peerStoppedSending(handle, code: code)
        }
    }

    /// Both directions of stream `id` are done, and the connection forgot it.
    func streamCloses(_ id: QUICStreamID) {
        if let table = self.table, let handle = table.handle(forID: id) {
            table.streamDisconnected(handle)
        }
    }

    /// The peer allows `count` more streams of a kind: queued ones open.
    func raiseLimit(bidirectional: Bool, by count: Int) {
        if bidirectional {
            self.bidirectionalLimit += count
        } else {
            self.unidirectionalLimit += count
        }
        let pending = self.pendingOpens
        self.pendingOpens = []
        for (handle, bidirectional) in pending {
            self.openStream(for: handle, bidirectional: bidirectional)
        }
    }

    // MARK: QUICStreamOperations

    func openStream(for handle: QUICStreamHandle, bidirectional: Bool) {
        let opened = bidirectional ? self.openedBidirectional : self.openedUnidirectional
        let limit = bidirectional ? self.bidirectionalLimit : self.unidirectionalLimit
        guard opened < limit, self.pendingOpens.allSatisfy({ $0.bidirectional != bidirectional }) else {
            self.pendingOpens.append((handle, bidirectional))
            return
        }
        // The low bits of a stream ID: who opened it, and whether it is unidirectional
        // (RFC 9000 Section 2.1).
        let typeBits = UInt64(self.role == .server ? 1 : 0) | (bidirectional ? 0 : 2)
        let id = QUICStreamID(rawValue: UInt64(opened) << 2 | typeBits)
        if bidirectional {
            self.openedBidirectional += 1
        } else {
            self.openedUnidirectional += 1
        }
        self.streams[id] = Stream()
        self.table?.assignID(id, to: handle)
        self.table?.markReady(handle: handle, events: .opened)
    }

    func cancelPendingOpen(_ handle: QUICStreamHandle) {
        self.pendingOpens.removeAll { $0.handle == handle }
    }

    func read<Result>(
        _ streamID: QUICStreamID,
        _ body: (RawSpan, _ isFinished: Bool) -> (consumed: Int, result: Result)
    ) -> Result? {
        guard var stream = self.streams[streamID] else { return nil }
        stream.reads += 1
        let (consumed, result) = stream.inbound.span.bytes.withUnsafeBytes { bytes in
            body(RawSpan(_unsafeBytes: bytes), stream.inboundFinished)
        }
        stream.inbound.removeFirst(consumed)
        self.streams[streamID] = stream
        return result
    }

    func writableBytes(_ streamID: QUICStreamID) -> Int {
        self.streams[streamID] == nil ? 0 : self.writableBytesPerStream
    }

    @discardableResult
    func write(_ bytes: RawSpan, to streamID: QUICStreamID, fin: Bool) -> Int {
        guard !self.refusesWrites, self.streams[streamID] != nil else { return 0 }
        bytes.withUnsafeBytes { self.streams[streamID]!.outbound.append(contentsOf: $0) }
        if fin {
            self.streams[streamID]!.finished = true
        }
        return bytes.byteCount
    }

    func finish(_ streamID: QUICStreamID) {
        self.streams[streamID]?.finished = true
    }

    func resetStream(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.streams[streamID]?.resetCode = code
    }

    func stopSending(_ streamID: QUICStreamID, code: QUICApplicationErrorCode) {
        self.streams[streamID]?.stopSendingCode = code
    }
}

@available(anyAppleOS 26, *)
extension TestStreamOperations.Stream {
    /// The bytes written, as a string.
    var outboundString: String {
        String(decoding: self.outbound, as: UTF8.self)
    }
}
