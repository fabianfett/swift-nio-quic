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
        let configuration = {
            var configuration = QUICConfiguration.forServer(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: publicKeyPath,
                    privateKeyFilePath: privateKeyPath
                ),
                applicationProtocols: ["http/0.9"]
            )
            configuration.maxIdleTimeout = Self.maxIdleTimeout
            return configuration
        }()

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
        try await Self.clientLearnsOfLostServerState(drivers: drivers, sendingToASpare: false)
    }

    /// The token of a spare connection ID the server issued after the handshake resets the
    /// connection as well (RFC 9000 Section 10.3.1): the client switches to the spare before the
    /// server loses its state.
    @available(anyAppleOS 26, *)
    @Test(arguments: [
        Drivers(client: .stateMachine, server: .stateMachine),
        Drivers(client: .swiftNetwork, server: .stateMachine),
    ])
    func testAStatelessResetForASpareConnectionIDResetsTheConnection(drivers: Drivers) async throws {
        try await Self.clientLearnsOfLostServerState(drivers: drivers, sendingToASpare: true)
    }

    @available(anyAppleOS 26, *)
    private static func clientLearnsOfLostServerState(drivers: Drivers, sendingToASpare: Bool) async throws {
        let serverDriver = QUICConnectionDriver(drivers.server)
        let serverDrivers = NIOLockedValueBox<[QUICConnectionDriver?]>([])
        let server = try await Self.bindServer(port: 0, driver: serverDriver, drivers: serverDrivers)
        let port = server.localAddress!.port!

        let recording = StreamRecording()
        let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
        let configuration = {
            var configuration = QUICConfiguration.forClient(
                verificationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKeyPath),
                applicationProtocols: ["http/0.9"]
            )
            configuration.maxIdleTimeout = Self.maxIdleTimeout
            return configuration
        }()
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
                try channel.pipeline.syncOperations.addHandler(ShortHeaderRecorder())
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

        if sendingToASpare {
            // The client retires the server's connection ID of the handshake, and sends to a spare.
            let handshakeID = try await Self.lastDestinationConnectionID(of: client)
            try await connection.channel.triggerUserOutboundEvent(QUICRequestRetireDCIDEvent(dcid: handshakeID)).get()
            let next = try #require(try await request())
            _ = await recording.visits { $0.handle == next && $0.events.contains(.closed) }
            let spare = try await Self.lastDestinationConnectionID(of: client)
            #expect(spare != handshakeID)
        }

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

    /// The server's connection ID in the last short header packet the client sent.
    @available(anyAppleOS 26, *)
    private static func lastDestinationConnectionID(of client: any Channel) async throws -> QUICConnectionID {
        let lastPacket = try await client.eventLoop.submit {
            try client.pipeline.syncOperations.handler(type: ShortHeaderRecorder.self).lastPacket
        }.get()
        let packet = try #require(lastPacket)
        // The connection ID follows the first byte (RFC 9000 Section 17.3.1).
        let bytes = try #require(
            packet.getBytes(at: packet.readerIndex + 1, length: Int(QUICConnectionID.randomIDLength))
        )
        return QUICConnectionID(bytes: bytes.span)
    }
}
