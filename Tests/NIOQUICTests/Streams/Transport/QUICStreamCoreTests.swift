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

#if DEBUG  // These tests rely on debug only API.

import NIOCore
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

@available(anyAppleOS 26, *)
private enum NoConsumer: QUICStreamConsumer {
    typealias StreamState = Never

    mutating func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) -> Never {
        fatalError("no streams are made")
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {}
}

/// A stream core on stream operations without a connection, as a state machine connection's.
@available(anyAppleOS 26, *)
private struct TestCore: ~Copyable {
    static var id: QUICStreamID { 0 }

    var core: QUICStreamCore
    let operations: TestStreamOperations<NoConsumer>

    /// A connected client bidirectional stream, or one waiting for its ID.
    init(connected: Bool = true) {
        self.operations = TestStreamOperations(role: .client)
        if connected {
            self.core = QUICStreamCore(id: nil)
            _ = self.core.connected(id: Self.id, direction: .bidirectional)
            // The peer's data arrives on stream 0.
            self.operations.receive("", on: Self.id)
        } else {
            self.core = QUICStreamCore(id: nil)
        }
    }

    /// The peer sent `chunks`, which the operations hold contiguously, as a connection does.
    func hold(_ chunks: [String], fin: Bool = false) {
        for chunk in chunks {
            self.operations.receive(chunk, on: Self.id)
        }
        if fin {
            self.operations.receive("", fin: true, on: Self.id)
        }
    }

    var holdsNothing: Bool {
        self.operations[Self.id]?.inbound.isEmpty ?? true
    }

    mutating func read(minContiguous: Int, _ body: (_ span: borrowing RawSpan) -> Int) -> QUICStreamReadOutcome {
        self.core.read(minContiguous: minContiguous, from: self.operations, id: Self.id, body)
    }

    mutating func expectFlushRefused(fin: Bool) {
        do {
            try self.core.flush(fin: fin, to: self.operations)
            Issue.record("Expected flush to be refused")
        } catch {
            ()  // Refused, as expected.
        }
    }
}

@available(anyAppleOS 26, *)
extension RawSpan {
    fileprivate var utf8String: String {
        self.withUnsafeBytes { String(decoding: $0, as: UTF8.self) }
    }
}

@Suite
struct QUICStreamCoreTests {
    @Test
    @available(anyAppleOS 26, *)
    func partialReadResumesAtOffset() {
        var stream = TestCore()
        stream.hold(["abcdef"])

        var seen = [String]()
        let outcome = stream.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return 2  // consume 2 bytes
        }

        #expect(outcome == .read(6))
        #expect(seen == ["abcdef", "cdef", "ef"])
        let nothingHeld = stream.holdsNothing
        #expect(nothingHeld)
    }

    @Test
    @available(anyAppleOS 26, *)
    func declinedBytesAreHeldForTheNextRead() {
        var stream = TestCore()
        stream.hold(["ab", "cd", "ef"])

        var seen = [String]()
        var calls = 0

        let first = stream.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            calls += 1
            return calls == 1 ? 1 : 0
        }

        #expect(first == .read(1))
        let needsAnotherVisit = stream.core.needsReadVisit
        #expect(needsAnotherVisit)

        let second = stream.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }

        #expect(second == .read(5))
        // The connection holds what arrived contiguously, so each call sees all that's left.
        #expect(seen == ["abcdef", "bcdef", "bcdef"])
        let nothingHeld = stream.holdsNothing
        #expect(nothingHeld)
    }

    @Test
    @available(anyAppleOS 26, *)
    func waitsForTheContiguousBytesAskedFor() {
        var stream = TestCore()
        stream.hold(["ab"])

        var seen = [String]()
        let tooFew = stream.read(minContiguous: 4) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }
        #expect(tooFew == .nothingAvailable)
        #expect(seen.isEmpty)

        stream.hold(["cd", "ef"])
        let first = stream.read(minContiguous: 4) { span in
            seen.append(span.utf8String)
            return 3
        }

        // Three bytes are left, fewer than asked for: they wait for the next read.
        #expect(first == .read(3))
        let needsAnotherVisit = stream.core.needsReadVisit
        #expect(needsAnotherVisit)

        let second = stream.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }

        #expect(second == .read(3))
        #expect(seen == ["abcdef", "def"])
    }

    @Test
    @available(anyAppleOS 26, *)
    func shortRunIsHandedOverAtTheEndOfTheStream() {
        var stream = TestCore()
        stream.hold(["ab"], fin: true)

        var seen = [String]()
        let outcome = stream.read(minContiguous: 4) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }

        #expect(outcome == .endOfStream(2))
        #expect(seen == ["ab"])
        let isReceiveOpen = stream.core.isReceiveOpen
        #expect(!isReceiveOpen)
    }

    @Test
    @available(anyAppleOS 26, *)
    func readWithNothingHeld() {
        var stream = TestCore()

        let outcome = stream.read(minContiguous: 1) { _ in
            Issue.record("read a stream with nothing held")
            return 0
        }

        #expect(outcome == .nothingAvailable)
    }

    @Test
    @available(anyAppleOS 26, *)
    func closeStopsBothDirections() {
        var stream = TestCore()
        stream.hold(["ab", "cd"])
        stream.core.close(on: stream.operations, id: TestCore.id)

        #expect(stream.operations[TestCore.id]?.resetCode == QUICApplicationErrorCode(0))
        #expect(stream.operations[TestCore.id]?.stopSendingCode == QUICApplicationErrorCode(0))
        let isSendOpen = stream.core.isSendOpen
        #expect(!isSendOpen)
    }

    @Test(arguments: [true, false])
    @available(anyAppleOS 26, *)
    func flushOnStreamWIthoutIDIsRefused(fin: Bool) {
        var stream = TestCore(connected: false)
        stream.core.write(ByteBuffer(string: "hello"))
        stream.expectFlushRefused(fin: fin)

        let stillQueued = stream.core._forTesting_hasPendingWrites
        #expect(stillQueued)
        stream.core.abandonLocally()
    }

    @Test
    @available(anyAppleOS 26, *)
    func flushHandsTheWritesOverInOrder() throws {
        var stream = TestCore()
        stream.core.write(ByteBuffer(string: "hello "))
        stream.core.withWritableSpan(count: 5) { span in
            for (index, byte) in "world".utf8.enumerated() {
                span.storeBytes(of: byte, toByteOffset: index, as: UInt8.self)
            }
            return 5
        }
        try stream.core.flush(fin: true, to: stream.operations)

        #expect(stream.operations[TestCore.id]?.outboundString == "hello world")
        #expect(stream.operations[TestCore.id]?.finished == true)
        let stillQueued = stream.core._forTesting_hasPendingWrites
        #expect(!stillQueued)
    }
}

#endif  // DEBUG
