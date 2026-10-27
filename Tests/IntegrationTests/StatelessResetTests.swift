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

import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import Testing

@testable import NIOQUIC

/// A client whose server lost the connection's state learns about it from the stateless reset the
/// server's next incarnation sends (RFC 9000 Section 10.3), instead of waiting for the idle timeout.
@Suite(.timeLimit(.minutes(1)))
struct StatelessResetTests {
    /// Shared by the server's incarnations, so the second derives the first's tokens.
    private static let statelessResetKey = [UInt8](repeating: 0x5A, count: 32)

    private static let maxIdleTimeout = Duration.seconds(10)

    @available(anyAppleOS 26, *)
    private static func bindServer(
        port: Int,
        driver: QUICConnectionDriver,
        drivers: NIOLockedValueBox<[QUICConnectionDriver?]>
    ) async throws -> any Channel {
        let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
        let privateKeyPath = Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path
        var configuration = QUICConfiguration.forServer(
            serverName: "quic-test.local",
            authenticationConfiguration: .rawPublicKeys(
                publicKeyFilePath: publicKeyPath,
                privateKeyFilePath: privateKeyPath
            ),
            applicationProtocols: ["http/0.9"]
        )
        configuration.maxIdleTimeout = Self.maxIdleTimeout

        return try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: port)
            .flatMapThrowing { channel in
                let handler = QUICHandler<EchoConsumer>(
                    channel: channel,
                    quicConfiguration: configuration,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "Server"),
                    statelessResetTokenGenerator: .defaultWithUserProvidedKey(Self.statelessResetKey),
                    makeConsumer: { connection in
                        drivers.withLockedValue { $0.append(connection.driver) }
                        return EchoConsumer()
                    }
                )
                handler.connectionDriver = driver
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testAClientLearnsOfLostServerStateFromAStatelessReset(drivers: Drivers) async throws {
        let serverDriver = QUICConnectionDriver(drivers.server)
        let serverDrivers = NIOLockedValueBox<[QUICConnectionDriver?]>([])
        let server = try await Self.bindServer(port: 0, driver: serverDriver, drivers: serverDrivers)
        let port = server.localAddress!.port!

        let recording = StreamRecording()
        let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
        var configuration = QUICConfiguration.forClient(
            verificationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKeyPath),
            applicationProtocols: ["http/0.9"]
        )
        configuration.maxIdleTimeout = Self.maxIdleTimeout
        let client = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<RecordConsumer>(
                    channel: channel,
                    quicConfiguration: configuration,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "Client"),
                    makeConsumer: { _ in RecordConsumer(recording: recording) }
                )
                handler.connectionDriver = QUICConnectionDriver(drivers.client)
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        defer { client.close(promise: nil) }

        let connection = try await client.pipeline.handler(type: QUICHandler<RecordConsumer>.self)
            .flatMap { handler in
                handler.createOutboundConnection(
                    serverName: "127.0.0.1:\(port)",
                    remoteAddress: server.localAddress!
                )
            }
            .get()
        #expect(connection.driver == QUICConnectionDriver(drivers.client))
        #expect(serverDrivers.withLockedValue { $0 } == [serverDriver])

        func request() async throws -> QUICStreamHandle? {
            try await connection.withStreams { streams in
                let handle = try streams.open(.clientInitiatedBidirectional, state: ByteBuffer())
                try streams.withStream(handle: handle) { stream, _ in
                    stream.write(ByteBuffer(string: "GET /"))
                    try stream.flush(fin: true)
                }
                return handle
            }.get()
        }

        // One round trip, so the connection is fully established.
        let first = try #require(try await request())
        let echoed = await recording.visits { $0.handle == first && $0.events.contains(.closed) }
        #expect(echoed.last?.bytesReadSoFar == ByteBuffer(string: "GET /"))

        // The server loses its state: closing its socket sends nothing, so the client doesn't
        // know. Its next incarnation, on the same port, has no state for the connection.
        try await server.close()
        let restarted = try await Self.bindServer(port: port, driver: serverDriver, drivers: serverDrivers)
        defer { restarted.close(promise: nil) }

        let start = ContinuousClock.now
        let second = try #require(try await request())
        let closed = await recording.visits { $0.handle == second && $0.events.contains(.closed) }
        let elapsed = ContinuousClock.now - start

        #expect(elapsed < Self.maxIdleTimeout / 2, "the client waited for its idle timeout: \(elapsed)")
        // The SwiftNetwork driver reports the reset as a clean close.
        if drivers.client == .stateMachine {
            #expect(closed.last?.closeErrorDescription?.contains("stateless reset") == true)
        }
    }
}
