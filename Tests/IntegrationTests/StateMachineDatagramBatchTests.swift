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
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

/// The bytes of the responses: a pattern that doesn't repeat with the datagram size, so a datagram
/// sent twice or out of place shows.
private let responsePattern: [UInt8] = (0..<(2 * 1024 * 1024)).map { UInt8(truncatingIfNeeded: $0 % 251) }

/// Answers each request with `responseSize` bytes of the response pattern.
@available(anyAppleOS 26, *)
struct PatternResponder: QUICStreamConsumer {
    typealias StreamState = Void

    private let response: ByteBuffer

    init(responseSize: Int) {
        self.response = ByteBuffer(bytes: responsePattern.prefix(responseSize))
    }

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let response = self.response
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, _ in
                reading: while true {
                    var request = ByteBuffer()
                    switch stream.read(into: &request) {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        if stream.isSendOpen {
                            stream.write(response)
                            try? stream.flush(fin: true)
                        }
                        break reading
                    }
                }
            }
        }
    }
}

/// Reads responses and checks them against the response pattern.
@available(anyAppleOS 26, *)
struct PatternChecker: QUICStreamConsumer {
    /// A response read to its end: its length, and where it first differs from the pattern.
    struct Response: Sendable, Equatable {
        var length = 0
        var firstMismatch: Int?
    }

    /// The response so far, `nil` once it ended.
    typealias StreamState = Response?

    let responses: AsyncStream<Response>.Continuation

    func makeStreamState(_ stream: inout QUICStream<Self>) -> Response? {
        Response()
    }

    /// Where `bytes`, at `offset` in a response, first differ from the pattern.
    private static func firstMismatch(in bytes: ByteBuffer, at offset: Int) -> Int? {
        guard bytes.readableBytes > 0 else { return nil }
        let expected = responsePattern.dropFirst(offset).prefix(bytes.readableBytes)
        // The fast path, as debug builds compare byte by byte slowly.
        let equal = bytes.withUnsafeReadableBytes { received in
            expected.withUnsafeBytes { expected in
                received.count == expected.count
                    && memcmp(received.baseAddress!, expected.baseAddress!, received.count) == 0
            }
        }
        guard !equal else { return nil }
        let index =
            zip(bytes.readableBytesView, expected).enumerated().first { $0.element.0 != $0.element.1 }?.offset
            ?? expected.count
        return offset + index
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let responses = self.responses
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, state in
                reading: while var response = state {
                    var buffer = ByteBuffer()
                    let result = stream.read(into: &buffer)
                    if response.firstMismatch == nil {
                        response.firstMismatch = Self.firstMismatch(in: buffer, at: response.length)
                    }
                    response.length += buffer.readableBytes
                    state = response
                    switch result {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        responses.yield(response)
                        state = nil
                    }
                }
            }
        }
    }
}

/// What a connection's channel wrote to the socket, datagram by datagram.
struct DatagramRecording: Sendable {
    /// The datagrams of the envelopes, in order; an envelope with a segment size is split into
    /// them as the kernel splits it.
    var datagrams: [ByteBuffer] = []
    /// The envelopes, and those with a segment size.
    var envelopes = 0
    var segmentedEnvelopes = 0
    /// Envelopes whose segments break the kernel's rule: every segment but the last is the
    /// segment size.
    var malformedEnvelopes = 0
    /// Writes that failed.
    var failedWrites: [String] = []
    /// The most bytes and datagrams in an envelope.
    var largestEnvelope = 0
    var largestRun = 0
}

/// Records the datagrams a channel writes. With `splitting`, it passes each segment of an envelope
/// with a segment size on as its own envelope, which a platform without UDP GSO can send.
final class DatagramRecorder: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundOut = AddressedEnvelope<ByteBuffer>

    let recording: NIOLockedValueBox<DatagramRecording>
    private let splitting: Bool

    init(recording: NIOLockedValueBox<DatagramRecording>, splitting: Bool) {
        self.recording = recording
        self.splitting = splitting
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let envelope = Self.unwrapOutboundIn(data)
        var segments: [ByteBuffer] = []
        var data = envelope.data
        let segmentSize = envelope.metadata?.segmentSize ?? data.readableBytes
        while data.readableBytes > 0 {
            // '!' is fine: there are that many bytes.
            segments.append(data.readSlice(length: min(segmentSize, data.readableBytes))!)
        }
        self.recording.withLockedValue { recording in
            recording.envelopes += 1
            recording.largestEnvelope = max(recording.largestEnvelope, envelope.data.readableBytes)
            recording.largestRun = max(recording.largestRun, segments.count)
            if envelope.metadata?.segmentSize != nil {
                recording.segmentedEnvelopes += 1
                if segments.dropLast().contains(where: { $0.readableBytes != segmentSize }) || segments.count < 2 {
                    recording.malformedEnvelopes += 1
                }
            }
            recording.datagrams.append(contentsOf: segments)
        }
        let recording = self.recording
        let written = context.eventLoop.makePromise(of: Void.self)
        written.futureResult.whenFailure { error in
            recording.withLockedValue { $0.failedWrites.append("\(error)") }
        }
        written.futureResult.cascade(to: promise)
        guard self.splitting, envelope.metadata?.segmentSize != nil else {
            context.write(Self.wrapOutboundOut(envelope), promise: written)
            return
        }
        for (index, segment) in segments.enumerated() {
            let split = AddressedEnvelope(remoteAddress: envelope.remoteAddress, data: segment)
            context.write(Self.wrapOutboundOut(split), promise: index == segments.count - 1 ? written : nil)
        }
    }
}

/// Gives the channel's socket a receive buffer large enough for the bursts of the tests: Linux
/// drops datagrams beyond its default of 208 KB, and losses keep the congestion window small.
final class LargeReceiveBuffer: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    func handlerAdded(context: ChannelHandlerContext) {
        _ = try? context.channel.syncOptions?.setOption(.socketOption(.so_rcvbuf), value: 4 * 1024 * 1024)
    }
}

/// A state machine connection writes the datagrams of a drain into one buffer, and sends them as
/// slices of it.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineDatagramBatchTests {
    /// Transfers a response of `responseSize` bytes, and checks that it arrives intact and that
    /// the server's channel wrote exactly the datagrams its connection built.
    @available(anyAppleOS 26, *)
    private func transfer(
        responseSize: Int,
        paced: Bool,
        delayed: Bool? = nil,
        splitting: Bool = false,
        maximumSegments: Int? = nil,
        host: String = "127.0.0.1",
        configureServer: @escaping @Sendable (inout QUICConfiguration) -> Void = { _ in },
        sourceLocation: SourceLocation = #_sourceLocation
    ) async throws -> (DatagramRecording, Counts) {
        let recording = NIOLockedValueBox(DatagramRecording())
        let written = NIOLockedValueBox<[ByteBuffer]>([])
        let (responses, continuation) = AsyncStream.makeStream(of: PatternChecker.Response.self)
        var iterator = responses.makeAsyncIterator()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<PatternResponder>?>(nil)
        // An RTT of 50 ms, for the pacer to spread the window over.
        let delayed = delayed ?? paced
        let configureClientPipeline: @Sendable (ChannelPipeline.SynchronousOperations) throws -> Void = { pipeline in
            try pipeline.addHandler(LargeReceiveBuffer())
            if delayed {
                try pipeline.addHandler(InboundDelay(.milliseconds(50)))
            }
        }

        let body: (ConsumerPair<PatternChecker, PatternResponder>) async throws -> (DatagramRecording, Counts) = {
            pair in
            // A first request completes the handshake.
            let request = ByteBuffer(string: "GET /")
            _ = try await pair.openClientStream(state: PatternChecker.Response(), writing: request, fin: true)
            let first = await iterator.next()
            #expect(first == PatternChecker.Response(length: responseSize), sourceLocation: sourceLocation)

            let server = try #require(serverConnection.withLockedValue { $0 })
            let before = try await Self.startRecording(
                server.channel,
                recording: recording,
                written: written,
                maximumSegments: maximumSegments
            )

            _ = try await pair.openClientStream(state: PatternChecker.Response(), writing: request, fin: true)
            let second = await iterator.next()
            #expect(second == PatternChecker.Response(length: responseSize), sourceLocation: sourceLocation)

            let (sent, counts) = try await Self.stopRecording(server.channel, recording: recording, since: before)
            let built = written.withLockedValue { $0 }
            // The channel sent the datagrams the connection built, in order, but for one it may
            // still hold for its departure.
            #expect(sent.datagrams == Array(built.prefix(sent.datagrams.count)), sourceLocation: sourceLocation)
            #expect((built.count - 1)...built.count ~= sent.datagrams.count, sourceLocation: sourceLocation)
            #expect(sent.malformedEnvelopes == 0, sourceLocation: sourceLocation)
            #expect(sent.failedWrites == [], sourceLocation: sourceLocation)
            // The drains sent all they didn't hold; one held before may have gone out too.
            #expect(counts.datagrams <= sent.datagrams.count, sourceLocation: sourceLocation)
            #expect(sent.datagrams.count <= counts.datagrams + counts.held + 1, sourceLocation: sourceLocation)
            if paced {
                // The pacer held datagrams back, which went out intact.
                #expect(counts.held > 0, sourceLocation: sourceLocation)
            }
            return (sent, counts)
        }

        return try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            host: host,
            configureServer: { configuration in
                configuration.pacePackets = paced
                configureServer(&configuration)
            },
            configureClientPipeline: configureClientPipeline,
            configureServerPipeline: { pipeline in
                try pipeline.addHandler(DatagramRecorder(recording: recording, splitting: splitting))
            },
            client: PatternChecker(responses: continuation),
            server: PatternResponder(responseSize: responseSize),
            onServerConnection: { connection in serverConnection.withLockedValue { $0 = connection } },
            body
        )
    }

    @available(anyAppleOS 26, *)
    private static func connection(_ channel: any Channel) -> StateMachineQUICConnection<PatternResponder> {
        (channel as! QUICConnectionChannel<PatternResponder>)._forTesting_stateMachineConnection!
    }

    /// From now on, records the datagrams the server connection writes in `written`, and those
    /// its channel sends in `recording`, alike: starting with one the pacer may hold.
    @available(anyAppleOS 26, *)
    private static func startRecording(
        _ channel: any Channel,
        recording: NIOLockedValueBox<DatagramRecording>,
        written: NIOLockedValueBox<[ByteBuffer]>,
        maximumSegments: Int?
    ) async throws -> Counts {
        try await channel.eventLoop.submit {
            let connection = Self.connection(channel)
            if let maximumSegments {
                connection._forTesting_maximumSegments = maximumSegments
            }
            recording.withLockedValue { $0 = DatagramRecording() }
            written.withLockedValue { $0 = connection._forTesting_heldDatagram.map { [$0] } ?? [] }
            connection._forTesting_onDatagramWritten = { datagram in
                written.withLockedValue { $0.append(datagram) }
            }
            return Counts(connection)
        }.get()
    }

    @available(anyAppleOS 26, *)
    private static func stopRecording(
        _ channel: any Channel,
        recording: NIOLockedValueBox<DatagramRecording>,
        since before: Counts
    ) async throws -> (DatagramRecording, Counts) {
        try await channel.eventLoop.submit {
            let connection = Self.connection(channel)
            connection._forTesting_onDatagramWritten = nil
            return (recording.withLockedValue { $0 }, Counts(connection) - before)
        }.get()
    }

    struct Counts: Sendable {
        var envelopes: Int
        var datagrams: Int
        var segmentedEnvelopes: Int
        var held: Int

        @available(anyAppleOS 26, *)
        init(_ connection: StateMachineQUICConnection<PatternResponder>) {
            self.envelopes = connection._forTesting_envelopesSent
            self.datagrams = connection._forTesting_datagramsSent
            self.segmentedEnvelopes = connection._forTesting_segmentedEnvelopesSent
            self.held = connection._forTesting_datagramsHeld
        }

        init(envelopes: Int, datagrams: Int, segmentedEnvelopes: Int, held: Int) {
            self.envelopes = envelopes
            self.datagrams = datagrams
            self.segmentedEnvelopes = segmentedEnvelopes
            self.held = held
        }

        static func - (lhs: Self, rhs: Self) -> Self {
            Self(
                envelopes: lhs.envelopes - rhs.envelopes,
                datagrams: lhs.datagrams - rhs.datagrams,
                segmentedEnvelopes: lhs.segmentedEnvelopes - rhs.segmentedEnvelopes,
                held: lhs.held - rhs.held
            )
        }
    }

    /// Whether the platform sends runs of datagrams with UDP GSO: NIO supports it on Linux.
    private static var segments: Bool {
        #if os(Linux) || os(Android)
        true
        #else
        false
        #endif
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [false, true])
    func aLargeResponseArrivesIntact(paced: Bool) async throws {
        let (sent, counts) = try await self.transfer(responseSize: 1024 * 1024, paced: paced)
        // Hundreds of full datagrams.
        #expect(sent.datagrams.count > 800, "\(sent.datagrams.count) datagrams")
        if Self.segments {
            // The kernel got runs of datagrams, which arrived as datagrams.
            #expect(sent.segmentedEnvelopes > 0)
            #expect(counts.segmentedEnvelopes == sent.segmentedEnvelopes)
            #expect(counts.datagrams > counts.envelopes)
        } else {
            // NIO only segments on Linux: every datagram is its own envelope.
            #expect(sent.segmentedEnvelopes == 0)
            #expect(counts.segmentedEnvelopes == 0)
            #expect(counts.datagrams == counts.envelopes)
        }
    }

    /// Runs of datagrams, split as the kernel splits them, arrive intact on every platform; paced,
    /// the datagram the pacer holds back is a slice of a buffer with runs.
    @available(anyAppleOS 26, *)
    @Test(arguments: [false, true])
    func runsOfDatagramsArriveIntact(paced: Bool) async throws {
        let (sent, counts) = try await self.transfer(
            responseSize: 1024 * 1024,
            paced: paced,
            splitting: true,
            maximumSegments: 64
        )
        #expect(sent.segmentedEnvelopes > 0)
        #expect(counts.segmentedEnvelopes > 0)
        #expect(counts.datagrams > counts.envelopes)
        // 54 datagrams of 1200 bytes are the most one UDP send carries.
        #expect(sent.largestRun <= 54)
        #expect(sent.largestEnvelope <= 65507)
    }

    /// The longest runs fit in one UDP send: 44 datagrams of 1489 bytes would be 65516 bytes, more
    /// than IPv4 carries, so a run has at most 43 and maybe one shorter. The kernel takes them,
    /// over IPv4 and IPv6.
    ///
    /// With an RTT, the second response starts with a drain of the window the first one opened,
    /// which has room for runs that long.
    @available(anyAppleOS 26, *)
    @Test(arguments: ["127.0.0.1", "::1"])
    func theLongestRunsFitOneSend(host: String) async throws {
        let (sent, _) = try await self.transfer(
            responseSize: 1024 * 1024,
            paced: false,
            delayed: true,
            splitting: !Self.segments,
            maximumSegments: 64,
            host: host,
            configureServer: { $0.initialPacketSize = .fixed(1489) }
        )
        #expect(sent.largestEnvelope <= 65507)
        // Runs got about as long as they can.
        #expect(sent.largestEnvelope > 43 * 1489, "\(sent.largestEnvelope) bytes")
        #expect(43...44 ~= sent.largestRun, "\(sent.largestRun) datagrams")
    }
}

#if os(Linux) || os(Android)
/// Collects the sizes of the datagrams a channel reads.
final class DatagramSizeCollector: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    let sizes: NIOLockedValueBox<[Int]>

    init(sizes: NIOLockedValueBox<[Int]>) {
        self.sizes = sizes
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let size = Self.unwrapInboundIn(data).data.readableBytes
        self.sizes.withLockedValue { $0.append(size) }
    }
}

/// The kernel segments a UDP send of up to `DatagramRuns.maximumUDPPayload` bytes, with and
/// without the don't fragment bit, over IPv4 and IPv6; a byte more is too much for IPv4.
@Suite(.timeLimit(.minutes(1)))
struct UDPSegmentationOffloadTests {
    @available(anyAppleOS 26, *)
    @Test(arguments: ["127.0.0.1", "::1"], [false, true])
    func theKernelTakesTheLongestRun(host: String, dontFragment: Bool) async throws {
        let sizes = NIOLockedValueBox<[Int]>([])
        let receiver = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(DatagramSizeCollector(sizes: sizes))
                }
            }
            .bind(host: host, port: 0)
            .get()
        let sender = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: host, port: 0)
            .get()
        if dontFragment {
            try await sender.eventLoop.submit { try QUICDontFragment.set(on: sender) }.get()
        }

        let segmentSize = 1489
        let send = { (length: Int) in
            let envelope = AddressedEnvelope(
                remoteAddress: receiver.localAddress!,
                data: ByteBuffer(repeating: 0x2a, count: length),
                metadata: .init(ecnState: .transportNotCapable, packetInfo: nil, segmentSize: segmentSize)
            )
            return sender.writeAndFlush(envelope)
        }
        let length = DatagramRuns.maximumUDPPayload
        #expect(length == 65507)
        try await send(length).get()
        let expected = Array(repeating: segmentSize, count: length / segmentSize) + [length % segmentSize]
        let deadline = ContinuousClock.now + .seconds(10)
        while sizes.withLockedValue({ $0.count }) < expected.count, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(sizes.withLockedValue { $0 } == expected)

        // IPv4 carries no more; IPv6 carries 20 bytes more.
        let tooLong = host.contains(":") ? 65528 : length + 1
        await #expect(throws: (any Error).self) { try await send(tooLong).get() }

        try await sender.close()
        try await receiver.close()
    }
}
#endif
#endif
