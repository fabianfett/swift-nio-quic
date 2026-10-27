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
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

/// Ignores its streams: the connections in these tests never get far enough to open any.
@available(anyAppleOS 26, *)
private struct IdleConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while streams.next() != nil {}
    }
}

/// Collects the datagrams a channel reads.
private final class DatagramCollector: ChannelInboundHandler, Sendable {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    let datagrams: AsyncStream<ByteBuffer>
    private let continuation: AsyncStream<ByteBuffer>.Continuation

    init() {
        (self.datagrams, self.continuation) = AsyncStream.makeStream()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.continuation.yield(Self.unwrapInboundIn(data).data)
    }
}

/// State machine connections closed before their handshake finishes are released: nothing, such as
/// the handler resuming an asynchronous certificate verification, keeps them alive.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineConnectionLifetimeTests {
    /// Waits for the state machine connection behind the connection in `box` to be released once
    /// `close` ran on its channel.
    ///
    /// Takes the connection out of `box`, so the test holds no reference to it while waiting.
    @available(anyAppleOS 26, *)
    private static func expectReleased<Consumer: QUICStreamConsumer>(
        _ box: NIOLockedValueBox<QUICStreamConnection<Consumer>?>,
        afterRunning close: @escaping @Sendable (_ connection: any Channel) -> EventLoopFuture<Void>
    ) async throws {
        let eventLoop = try #require(box.withLockedValue { $0?.eventLoop })
        let released = eventLoop.makePromise(of: Void.self)
        let timeout = eventLoop.scheduleTask(in: .seconds(5)) {
            released.fail(TimedOut())
        }
        try await eventLoop.flatSubmit {
            let channel = box.withLockedValue { $0.take() }!.channel as! QUICConnectionChannel<Consumer>
            channel._forTesting_stateMachineConnection!._forTesting_onDeinit = { released.succeed() }
            return close(channel)
        }.get()

        await #expect(throws: Never.self) { try await released.futureResult.get() }
        timeout.cancel()
    }

    private struct TimedOut: Error {}

    @Test
    @available(anyAppleOS 26, *)
    func aClientClosedDuringItsHandshakeIsReleased() async throws {
        let certificates = try TestCertificates()
        let paths = try certificates.writeToTemp(fileTag: "aClientClosedDuringItsHandshakeIsReleased-\(UUID())")
        // A peer that never answers, so the handshake stays in flight.
        let blackHole = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .get()
        defer { blackHole.close(promise: nil) }

        let connections = NIOLockedValueBox<QUICStreamConnection<IdleConsumer>?>(nil)
        let configuration = QUICConfiguration.forClient(
            verificationConfiguration: .x509Certificates(trustRootsFilePath: paths.trustStoreFilePath),
            applicationProtocols: ["echo"]
        )
        let client = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<IdleConsumer>(
                    channel: channel,
                    quicConfiguration: configuration,
                    asyncVerifier: try AsyncVerifier(
                        trustRootsPath: paths.trustStoreFilePath,
                        eventLoop: channel.eventLoop
                    ),
                    authenticator: nil,
                    logger: Logger(label: "Client"),
                    makeConsumer: { connection in
                        connections.withLockedValue { $0 = connection }
                        return IdleConsumer()
                    }
                )
                handler.connectionDriver = .stateMachine
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        defer { client.close(promise: nil) }

        // The connection exists once this returns; it completes when the handshake does.
        let connected = try await client.pipeline.handler(type: QUICHandler<IdleConsumer>.self)
            .map { handler in
                handler.createOutboundConnection(
                    serverName: certificates.leafName,
                    remoteAddress: blackHole.localAddress!
                )
            }
            .get()
        #expect(connections.withLockedValue { $0?.driver } == .stateMachine)

        try await Self.expectReleased(connections) { channel in channel.close() }
        await #expect(throws: (any Error).self) { try await connected.get() }
    }

    @Test
    @available(anyAppleOS 26, *)
    func aServerShutDownDuringAHandshakeReleasesTheConnection() async throws {
        let certificates = try TestCertificates()
        let paths = try certificates.writeToTemp(
            fileTag: "aServerShutDownDuringAHandshakeReleasesTheConnection-\(UUID())"
        )
        let connections = NIOLockedValueBox<QUICStreamConnection<IdleConsumer>?>(nil)
        let serverConfiguration = QUICConfiguration.forServer(
            serverName: certificates.leafName,
            authenticationConfiguration: .x509Certificates(
                certificateChainFilePath: paths.serverCertFilePath,
                privateKeyFilePath: paths.serverPrivateKeyFilePath
            ),
            applicationProtocols: ["echo"]
        )
        let server = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<IdleConsumer>(
                    channel: channel,
                    quicConfiguration: serverConfiguration,
                    asyncVerifier: nil,
                    authenticator: try Authenticator(
                        certificateFilePath: paths.serverCertFilePath,
                        privateKeyFilePath: paths.serverPrivateKeyFilePath
                    ),
                    logger: Logger(label: "Server"),
                    makeConsumer: { connection in
                        connections.withLockedValue { $0 = connection }
                        return IdleConsumer()
                    }
                )
                handler.connectionDriver = .stateMachine
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()

        // Capture a client's Initial, then replay it from a socket that ignores the server's answer:
        // the server's handshake stays in flight.
        let collector = DatagramCollector()
        let capture = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(collector)
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        defer { capture.close(promise: nil) }
        let clientConfiguration = QUICConfiguration.forClient(
            verificationConfiguration: .x509Certificates(trustRootsFilePath: paths.trustStoreFilePath),
            applicationProtocols: ["echo"]
        )
        let client = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<IdleConsumer>(
                    channel: channel,
                    quicConfiguration: clientConfiguration,
                    asyncVerifier: try AsyncVerifier(
                        trustRootsPath: paths.trustStoreFilePath,
                        eventLoop: channel.eventLoop
                    ),
                    authenticator: nil,
                    logger: Logger(label: "Client"),
                    makeConsumer: { _ in IdleConsumer() }
                )
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        let connected = try await client.pipeline.handler(type: QUICHandler<IdleConsumer>.self)
            .map { handler in
                handler.createOutboundConnection(
                    serverName: certificates.leafName,
                    remoteAddress: capture.localAddress!
                )
            }
            .get()
        connected.whenFailure { _ in }
        var datagrams = collector.datagrams.makeAsyncIterator()
        let initial = try #require(await datagrams.next())
        try await client.close()

        let replay = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .get()
        defer { replay.close(promise: nil) }
        try await replay.writeAndFlush(AddressedEnvelope(remoteAddress: server.localAddress!, data: initial))

        // The server accepts the connection when the Initial arrives.
        while connections.withLockedValue({ $0 == nil }) {
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(connections.withLockedValue { $0?.driver } == .stateMachine)

        try await Self.expectReleased(connections) { _ in server.close() }
    }
}
#endif
