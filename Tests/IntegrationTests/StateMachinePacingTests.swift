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

#if DEBUG
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

/// Counts the datagrams written between two flushes: the burst a connection sends at once.
final class BurstCounter: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundOut = AddressedEnvelope<ByteBuffer>

    /// The largest burst so far.
    let largestBurst: NIOLockedValueBox<Int>
    private var burst = 0

    init(largestBurst: NIOLockedValueBox<Int>) {
        self.largestBurst = largestBurst
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        self.burst += 1
        context.write(data, promise: promise)
    }

    func flush(context: ChannelHandlerContext) {
        let burst = self.burst
        self.burst = 0
        self.largestBurst.withLockedValue { $0 = max($0, burst) }
        context.flush()
    }
}

/// Reads its streams to their end and reports how many bytes each had, once.
@available(anyAppleOS 26, *)
struct ByteCountingConsumer: QUICStreamConsumer {
    /// The bytes read so far, `nil` once the stream ended.
    typealias StreamState = Int?

    let finished: AsyncStream<Int>.Continuation

    func makeStreamState(_ stream: inout QUICStream<Self>) -> Int? {
        0
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let finished = self.finished
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, count in
                reading: while let bytesSoFar = count {
                    var buffer = ByteBuffer()
                    switch stream.read(into: &buffer) {
                    case .read:
                        count = bytesSoFar + buffer.readableBytes
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        finished.yield(bytesSoFar + buffer.readableBytes)
                        count = nil
                    }
                }
            }
        }
    }
}

/// A state machine connection paces what it sends (RFC 9002 Section 7.7): a window that opens at
/// once goes out over the RTT, not in one burst.
@Suite(.timeLimit(.minutes(1)))
struct StateMachinePacingTests {
    @available(anyAppleOS 26, *)
    @Test
    func aWindowThatOpensAtOnceGoesOutPaced() async throws {
        let chunkSize = 64 * 1024
        let chunkCount = 32
        let responseSize = chunkSize * chunkCount + "Success".utf8.count
        let largestBurst = NIOLockedValueBox(0)
        let (finished, continuation) = AsyncStream.makeStream(of: Int.self)
        var responses = finished.makeAsyncIterator()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<StreamingConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<StreamingConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }

        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            // An RTT of 50 ms: the pacer sends a window per RTT, twice that in slow start.
            configureClientPipeline: { try $0.addHandler(InboundDelay(.milliseconds(50))) },
            configureServerPipeline: { try $0.addHandler(BurstCounter(largestBurst: largestBurst)) },
            client: ByteCountingConsumer(finished: continuation),
            server: StreamingConsumer(chunkSize: chunkSize, chunkCount: chunkCount),
            onServerConnection: captureConnection
        ) { pair in
            // The first download grows the congestion window.
            _ = try await pair.openClientStream(state: 0, writing: ByteBuffer(string: "GET /quic"), fin: true)
            #expect(await responses.next() == responseSize)

            let server = try #require(serverConnection.withLockedValue { $0 })
            let (window, datagramSize) = try await server.channel.eventLoop.submit {
                let connection = (server.channel as! QUICConnectionChannel<StreamingConsumer>)
                    ._forTesting_stateMachineConnection!
                return (connection._forTesting_congestionWindow, connection._forTesting_maximumDatagramSize)
            }.get()
            // The window is well past the initial ten datagrams, so the test means something.
            try #require(window > 100 * datagramSize, "window \(window)")

            // The second response could use all of the window at once.
            largestBurst.withLockedValue { $0 = 0 }
            _ = try await pair.openClientStream(state: 0, writing: ByteBuffer(string: "GET /quic"), fin: true)
            #expect(await responses.next() == responseSize)

            // Paced, a burst is about a millisecond of the rate (CorePacer), at most a 25th of
            // the window at this RTT; a drain that runs late sends two. Unpaced, it is the whole
            // window.
            let burst = largestBurst.withLockedValue { $0 }
            #expect(burst * datagramSize <= window / 4, "\(burst) datagrams at once, window \(window)")
        }
    }
}
#endif
