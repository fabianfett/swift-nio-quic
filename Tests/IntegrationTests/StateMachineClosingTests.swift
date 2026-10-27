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

/// Delays every datagram the channel reads, so a connection's RTT, and its PTO, is at least that
/// long.
final class InboundDelay: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias InboundOut = AddressedEnvelope<ByteBuffer>

    let delay: TimeAmount

    init(_ delay: TimeAmount) {
        self.delay = delay
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.eventLoop.assumeIsolated().scheduleTask(in: self.delay) {
            context.fireChannelRead(data)
            context.fireChannelReadComplete()
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        // Each delayed read completes on its own.
    }
}

/// Drops the next datagrams the channel writes.
final class OutboundDropper: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundOut = AddressedEnvelope<ByteBuffer>

    var dropNext = 0

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        if self.dropNext > 0 {
            self.dropNext -= 1
            promise?.succeed()
            return
        }
        context.write(data, promise: promise)
    }
}

/// A state machine connection reports that it closes as soon as its core starts closing or
/// draining, not once the closing or draining period of three PTOs is over (RFC 9000 Section
/// 10.2), and its core runs that period out after the channel is gone.
///
/// The client reads every datagram late, which makes its PTO, and its closing period, long.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineClosingTests {
    /// The client's inbound delay: its PTO is at least this plus the peer's 25 ms max_ack_delay,
    /// so its closing period is at least 975 ms.
    static let delay = TimeAmount.milliseconds(300)

    static let delayClientReads: @Sendable (ChannelPipeline.SynchronousOperations) throws -> Void = {
        try $0.addHandler(InboundDelay(Self.delay))
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [
        Drivers(client: .stateMachine, server: .stateMachine),
        Drivers(client: .stateMachine, server: .swiftNetwork),
    ])
    func thePeersApplicationCloseReachesTheConsumerAtOnce(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }

        try await withOwnEventLoops { eventLoopGroup in
            try await withConsumerPair(
                drivers: drivers,
                eventLoopGroup: eventLoopGroup,
                configureClientPipeline: Self.delayClientReads,
                client: RecordConsumer(recording: recording),
                server: EchoConsumer(),
                onServerConnection: captureConnection
            ) { pair in
                // The server only echoes once it processed the client's Finished, so its close
                // can't land in a Handshake packet.
                _ = try await pair.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
                _ = await recording.visits(untilClosedStreams: 1)
                _ = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

                let connection = try #require(serverConnection.withLockedValue { $0 })
                let start = ContinuousClock.now
                try await connection.channel.triggerUserOutboundEvent(
                    QUICCloseConnectionEvent(code: QUICApplicationErrorCode(10)!, reasonPhrase: "test")
                )
                let visits = await recording.visits { $0.events.contains(.closed) }
                let elapsed = ContinuousClock.now - start

                let error = try #require(visits.last?.closeError as? QUICConnectionError)
                #expect(error.code == 10)
                #expect(error.isApplication)
                // The CONNECTION_CLOSE takes the delay to arrive; the draining period would add
                // three PTOs, at least 975 ms.
                #expect(elapsed < .milliseconds(700), "the close took \(elapsed)")
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aLocalTransportErrorReachesTheApplicationAtOnce() async throws {
        // The P-256 generator as a public key: not the server's key, so the client's TLS rejects
        // the server and closes with a transport error (RFC 9001 Section 4.8).
        let wrongKey: [UInt8] =
            [
                0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
                0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00, 0x04,
            ] + [
                0x6b, 0x17, 0xd1, 0xf2, 0xe1, 0x2c, 0x42, 0x47, 0xf8, 0xbc, 0xe6, 0xe5, 0x63, 0xa4, 0x40, 0xf2,
                0x77, 0x03, 0x7d, 0x81, 0x2d, 0xeb, 0x33, 0xa0, 0xf4, 0xa1, 0x39, 0x45, 0xd8, 0x98, 0xc2, 0x96,
                0x4f, 0xe3, 0x42, 0xe2, 0xfe, 0x1a, 0x7f, 0x9b, 0x8e, 0xe7, 0xeb, 0x4a, 0x7c, 0x0f, 0x9e, 0x16,
                0x2b, 0xce, 0x33, 0x57, 0x6b, 0x31, 0x5e, 0xce, 0xcb, 0xb6, 0x40, 0x68, 0x37, 0xbf, 0x51, 0xf5,
            ]
        let keyURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("aLocalTransportErrorReachesTheApplicationAtOnce-\(UUID()).der")
        try Data(wrongKey).write(to: keyURL)
        defer { try? FileManager.default.removeItem(at: keyURL) }

        try await withOwnEventLoops { eventLoopGroup in
            let start = ContinuousClock.now
            await #expect(throws: QUICConnectionError.self) {
                let pair = try await makeConsumerPair(
                    drivers: Drivers(client: .stateMachine, server: .stateMachine),
                    eventLoopGroup: eventLoopGroup,
                    configureClient: { configuration in
                        configuration.verificationConfiguration = .rawPublicKeys(publicKeyFilePath: keyURL.path)
                    },
                    configureClientPipeline: Self.delayClientReads,
                    makeClientConsumer: { _ in RecordConsumer(recording: StreamRecording()) },
                    makeServerConsumer: { _ in EchoConsumer() }
                )
                try? await pair.close()
            }
            let elapsed = ContinuousClock.now - start

            // The server's first flight takes the delay to arrive. The closing period would add
            // three PTOs: with one RTT sample a PTO is three RTTs (RFC 9002 Section 6.2.1), so
            // 2.7 s.
            #expect(elapsed < .seconds(1), "the failure took \(elapsed)")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aConnectionClosedByThePeerIsReleasedAfterItsDrainingPeriod() async throws {
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let recording = StreamRecording()
        var pair: ConsumerPair<RecordConsumer, EchoConsumer>? = try await makeConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            configureClientPipeline: Self.delayClientReads,
            makeClientConsumer: { _ in RecordConsumer(recording: recording) },
            makeServerConsumer: { connection in
                serverConnection.withLockedValue { $0 = connection }
                return EchoConsumer()
            }
        )
        let clientChannel = pair!.clientChannel
        let serverChannel = pair!.serverChannel
        defer {
            clientChannel.close(promise: nil)
            serverChannel.close(promise: nil)
        }

        _ = try await pair!.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
        _ = await recording.visits(untilClosedStreams: 1)

        // Hold nothing that keeps the client connection alive.
        let box = NIOLockedValueBox(pair?.clientConnection)
        pair = nil
        let released = clientChannel.eventLoop.makePromise(of: ContinuousClock.Instant.self)
        let timeout = clientChannel.eventLoop.scheduleTask(in: .seconds(10)) {
            released.fail(TimedOut(description: "the client connection was never released"))
        }
        defer { timeout.cancel() }
        let channelClosed = try await clientChannel.eventLoop.submit {
            let channel = box.withLockedValue { $0.take() }!.channel as! QUICConnectionChannel<RecordConsumer>
            channel._forTesting_stateMachineConnection!._forTesting_onDeinit = {
                released.succeed(ContinuousClock.now)
            }
            return channel.closeFuture.map { ContinuousClock.now }
        }.get()

        let server = try #require(serverConnection.withLockedValue { $0 })
        try await server.channel.triggerUserOutboundEvent(
            QUICCloseConnectionEvent(code: QUICApplicationErrorCode(10)!, reasonPhrase: "test")
        )

        let closedAt = try await channelClosed.get()
        let releasedAt = try await released.futureResult.get()
        // The core drains for three PTOs, at least 975 ms, after the channel is gone.
        #expect(releasedAt - closedAt > .milliseconds(300), "released \(releasedAt - closedAt) after the close")
    }

    @available(anyAppleOS 26, *)
    @Test
    func aConnectionWhoseChannelIsGoneAnswersThePeerWhileClosing() async throws {
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let recording = StreamRecording()
        let pair = try await makeConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            // The server's PINGs reach the client while it closes.
            configureServer: { $0.keepAliveInterval = .milliseconds(100) },
            configureClientPipeline: {
                try Self.delayClientReads($0)
                try $0.addHandler(OutboundDropper())
            },
            makeClientConsumer: { _ in RecordConsumer(recording: recording) },
            makeServerConsumer: { connection in
                serverConnection.withLockedValue { $0 = connection }
                return EchoConsumer()
            }
        )
        defer {
            pair.clientChannel.close(promise: nil)
            pair.serverChannel.close(promise: nil)
        }
        _ = try await pair.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
        _ = await recording.visits(untilClosedStreams: 1)
        let server = try #require(serverConnection.withLockedValue { $0 })

        // The client's first CONNECTION_CLOSE is lost: the server only learns of the close from
        // the CONNECTION_CLOSE the client answers its next packet with, after the client's channel
        // is gone. Without it, the server waits for its 30 second idle timeout.
        let clientChannel = pair.clientChannel
        let connectionChannel = pair.clientConnection.channel
        try await clientChannel.eventLoop.submit {
            try clientChannel.pipeline.syncOperations.handler(type: OutboundDropper.self).dropNext = 1
            connectionChannel.close(promise: nil)
        }.get()
        try await connectionChannel.closeFuture.get()

        let serverClosed = makePromise(of: Void.self, timeout: .seconds(5))
        server.channel.closeFuture.cascade(to: serverClosed)
        try await serverClosed.futureResult.get()
    }
}
#endif
