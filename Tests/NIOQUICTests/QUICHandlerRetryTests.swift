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

import Logging
import NIOCore
import NIOEmbedded
import Testing
import XCTest

@testable import NIOQUIC

/// A consumer for connections whose streams a test never touches.
@available(anyAppleOS 26, *)
private struct RetryTestConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    func processStreams(_ streams: inout QUICStreamIterator<Self>) {}
}

/// Suggests the client's destination connection ID as the server's, which RFC 9000 allows for
/// connections without a Retry.
@available(anyAppleOS 26, *)
private struct EchoingGenerator: QUICConnectionID.Generator {
    var random = QUICConnectionID.RandomGenerator()

    var connectionIDLength: Int { self.random.connectionIDLength }

    mutating func next() -> QUICConnectionID {
        self.random.next()
    }

    mutating func next(sourceConnectionID: QUICConnectionID, destinationConnectionID: QUICConnectionID) -> QUICConnectionID {
        destinationConnectionID
    }
}

/// A server handler which sends Retry packets, on an embedded channel.
@available(anyAppleOS 26, *)
private final class RetryServer {
    private let channel: EmbeddedChannel
    let handler: QUICHandler<RetryTestConsumer>
    private var random = SystemRandomNumberGenerator()
    let clientAddress: SocketAddress

    /// A Retry the handler sent.
    struct Retry {
        var destinationConnectionID: QUICConnectionID
        var sourceConnectionID: QUICConnectionID
        var token: [UInt8]
        var size: Int
    }

    init(
        connectionLimits: QUICConnectionLimits = .unlimited,
        connectionIDGenerator: any QUICConnectionID.Generator = QUICConnectionID.RandomGenerator()
    ) throws {
        self.channel = EmbeddedChannel()
        self.channel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        self.clientAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 443)
        var configuration = QUICConfiguration.forServer(
            serverName: "quic-test.local",
            authenticationConfiguration: .rawPublicKeys(
                publicKeyFilePath: XCTestCase.testPublicKeyPath,
                privateKeyFilePath: XCTestCase.testPrivateKeyPath
            ),
            applicationProtocols: []
        )
        configuration.sendRetry = true
        self.handler = QUICHandler<RetryTestConsumer>(
            channel: self.channel,
            quicConfiguration: configuration,
            connectionLimits: connectionLimits,
            asyncVerifier: nil,
            authenticator: nil,
            logger: Logger(label: "Test"),
            connectionIDGenerator: connectionIDGenerator
        ) { _ in RetryTestConsumer() }
        try self.channel.pipeline.syncOperations.addHandler(self.handler)
    }

    deinit {
        _ = try? self.channel.finish()
    }

    func randomConnectionID() -> QUICConnectionID {
        QUICConnectionID.random(length: 8, using: &self.random)
    }

    /// Fires a version 1 Initial of `size` bytes from `address` and returns the datagrams the
    /// handler answers with.
    func fireInitial(
        destinationConnectionID: QUICConnectionID,
        sourceConnectionID: QUICConnectionID,
        token: [UInt8] = [],
        size: Int = 1200,
        from address: SocketAddress? = nil
    ) throws -> [AddressedEnvelope<ByteBuffer>] {
        var packet = QUICPackets.initial(
            destinationID: destinationConnectionID,
            sourceID: sourceConnectionID,
            token: token,
            version: 1
        )
        packet += [UInt8](repeating: 0, count: size - packet.count)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope(remoteAddress: address ?? self.clientAddress, data: ByteBuffer(bytes: packet))
        )
        self.channel.pipeline.fireChannelReadComplete()
        var answers: [AddressedEnvelope<ByteBuffer>] = []
        while let answer = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self) {
            answers.append(answer)
        }
        return answers
    }

    /// Parses the Retry in `envelope` (RFC 9000 Section 17.2.5).
    func parseRetry(_ envelope: AddressedEnvelope<ByteBuffer>) throws -> Retry {
        var bytes = envelope.data
        let size = bytes.readableBytes
        // Long header, fixed bit, and type 3 in version 1.
        let firstByte = bytes.readInteger(as: UInt8.self)
        #expect(firstByte.map { $0 & 0xf0 } == 0xf0)
        let version = bytes.readInteger(as: UInt32.self)
        #expect(version == 1)
        let destinationLength = bytes.readInteger(as: UInt8.self) ?? 0
        let destinationBytes = bytes.readBytes(length: Int(destinationLength))
        let destination = try #require(destinationBytes)
        let sourceLength = bytes.readInteger(as: UInt8.self) ?? 0
        let sourceBytes = bytes.readBytes(length: Int(sourceLength))
        let source = try #require(sourceBytes)
        // The token, and the 16-byte Retry Integrity Tag (RFC 9001 Section 5.8).
        let tokenLength = bytes.readableBytes - 16
        let tokenBytes = bytes.readBytes(length: tokenLength)
        let token = try #require(tokenBytes)
        return Retry(
            destinationConnectionID: QUICConnectionID(bytes: destination.span),
            sourceConnectionID: QUICConnectionID(bytes: source.span),
            token: token,
            size: size
        )
    }

    /// Whether `connectionID` routes to a connection: a short header packet to one that doesn't
    /// is answered with a stateless reset.
    func isRoutable(_ connectionID: QUICConnectionID) throws -> Bool {
        let probe = QUICPackets.shortHeader(destinationID: connectionID, payloadLength: 31)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope(remoteAddress: self.clientAddress, data: ByteBuffer(bytes: probe))
        )
        self.channel.pipeline.fireChannelReadComplete()
        return try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self) == nil
    }

    /// Sends an Initial without a token and returns the Retry it gets.
    func retry(
        destinationConnectionID: QUICConnectionID,
        sourceConnectionID: QUICConnectionID
    ) throws -> Retry {
        let answers = try self.fireInitial(
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: sourceConnectionID
        )
        try #require(answers.count == 1)
        return try self.parseRetry(answers[0])
    }
}

/// The stateless Retry a server's handler sends for state machine connections (RFC 9000 Section
/// 8.1.2).
struct QUICHandlerRetryTests {
    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithoutATokenGetsARetry() throws {
        let server = try RetryServer()
        let destinationConnectionID = server.randomConnectionID()
        let clientConnectionID = server.randomConnectionID()

        let retry = try server.retry(
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: clientConnectionID
        )

        #expect(retry.destinationConnectionID == clientConnectionID)
        #expect(retry.sourceConnectionID != destinationConnectionID)
        #expect(retry.sourceConnectionID.length == 8)
        #expect(retry.size < 1200)
        // The token authenticates the client's first destination connection ID.
        let tokens = try #require(server.handler._forTesting_retryTokens)
        #expect(
            tokens.validate(
                retry.token,
                retrySourceConnectionID: retry.sourceConnectionID,
                clientAddress: server.clientAddress,
                now: .now()
            ) == .valid(originalDestinationConnectionID: destinationConnectionID)
        )
        #expect(server.handler._forTesting_retriesSent == 1)
        // The handler keeps no state for the client yet.
        #expect(try !server.isRoutable(destinationConnectionID))
        #expect(try !server.isRoutable(retry.sourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithTheTokenStartsAConnection() throws {
        let server = try RetryServer()
        let clientConnectionID = server.randomConnectionID()
        let retry = try server.retry(
            destinationConnectionID: server.randomConnectionID(),
            sourceConnectionID: clientConnectionID
        )

        _ = try server.fireInitial(
            destinationConnectionID: retry.sourceConnectionID,
            sourceConnectionID: clientConnectionID,
            token: retry.token
        )

        // No second Retry: a connection with the Retry's connection ID.
        #expect(server.handler._forTesting_retriesSent == 1)
        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 1)
        #expect(try server.isRoutable(retry.sourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithAnExpiredTokenIsDropped() throws {
        let server = try RetryServer()
        let destinationConnectionID = server.randomConnectionID()
        let clientConnectionID = server.randomConnectionID()
        let retrySourceConnectionID = server.randomConnectionID()
        let tokens = try #require(server.handler._forTesting_retryTokens)
        let token = tokens.makeToken(
            originalDestinationConnectionID: destinationConnectionID,
            retrySourceConnectionID: retrySourceConnectionID,
            clientAddress: server.clientAddress,
            now: .now() - QUICRetryTokens.lifetime - .seconds(1)
        )

        let answers = try server.fireInitial(
            destinationConnectionID: retrySourceConnectionID,
            sourceConnectionID: clientConnectionID,
            token: token
        )

        // RFC 9000 Section 8.1.3: the client accepts no second Retry, so the server drops it.
        #expect(answers.isEmpty)
        #expect(server.handler._forTesting_retriesSent == 0)
        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 0)
        #expect(try !server.isRoutable(retrySourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithAForgedTokenIsDropped() throws {
        let server = try RetryServer()
        let clientConnectionID = server.randomConnectionID()
        let retry = try server.retry(
            destinationConnectionID: server.randomConnectionID(),
            sourceConnectionID: clientConnectionID
        )
        var forged = retry.token
        forged[forged.count - 1] ^= 0x01

        let answers = try server.fireInitial(
            destinationConnectionID: retry.sourceConnectionID,
            sourceConnectionID: clientConnectionID,
            token: forged
        )

        #expect(answers.isEmpty)
        #expect(server.handler._forTesting_retriesSent == 1)
        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 0)
        #expect(try !server.isRoutable(retry.sourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithTheTokenOfAnotherAddressIsDropped() throws {
        let server = try RetryServer()
        let clientConnectionID = server.randomConnectionID()
        let retry = try server.retry(
            destinationConnectionID: server.randomConnectionID(),
            sourceConnectionID: clientConnectionID
        )

        let answers = try server.fireInitial(
            destinationConnectionID: retry.sourceConnectionID,
            sourceConnectionID: clientConnectionID,
            token: retry.token,
            from: SocketAddress(ipAddress: "127.0.0.2", port: 443)
        )

        #expect(answers.isEmpty)
        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 0)
        #expect(try !server.isRoutable(retry.sourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithTheTokenOfAnotherRetryIsDropped() throws {
        let server = try RetryServer()
        let clientConnectionID = server.randomConnectionID()
        let retry = try server.retry(
            destinationConnectionID: server.randomConnectionID(),
            sourceConnectionID: clientConnectionID
        )
        let otherConnectionID = server.randomConnectionID()

        let answers = try server.fireInitial(
            destinationConnectionID: otherConnectionID,
            sourceConnectionID: clientConnectionID,
            token: retry.token
        )

        #expect(answers.isEmpty)
        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 0)
        #expect(try !server.isRoutable(otherConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialWithAnotherKindOfTokenGetsARetry() throws {
        let server = try RetryServer()
        let destinationConnectionID = server.randomConnectionID()

        // RFC 9000 Section 8.1.3: the client's address isn't validated, so validate it.
        let answers = try server.fireInitial(
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: server.randomConnectionID(),
            token: [1, 2, 3, 4]
        )

        try #require(answers.count == 1)
        #expect(try server.parseRetry(answers[0]).sourceConnectionID != destinationConnectionID)
        #expect(server.handler._forTesting_retriesSent == 1)
        #expect(try !server.isRoutable(destinationConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func anInitialInASmallDatagramGetsNoRetry() throws {
        let server = try RetryServer()
        let destinationConnectionID = server.randomConnectionID()

        // RFC 9000 Section 14.1: a server discards Initials in datagrams under 1200 bytes.
        let answers = try server.fireInitial(
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: server.randomConnectionID(),
            size: 1199
        )

        #expect(answers.isEmpty)
        #expect(server.handler._forTesting_retriesSent == 0)
        #expect(try !server.isRoutable(destinationConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func retriesTakeNoAdmissionSlots() throws {
        let server = try RetryServer(connectionLimits: .perHandler(activeLimit: 1))
        let clientConnectionID = server.randomConnectionID()
        // Several Retries, for one connection.
        var retries: [RetryServer.Retry] = []
        for _ in 0..<3 {
            retries.append(
                try server.retry(destinationConnectionID: server.randomConnectionID(), sourceConnectionID: clientConnectionID)
            )
        }

        _ = try server.fireInitial(
            destinationConnectionID: retries[2].sourceConnectionID,
            sourceConnectionID: clientConnectionID,
            token: retries[2].token
        )

        #expect(server.handler._forTesting_connectionsAcceptedAfterRetry == 1)
        #expect(try server.isRoutable(retries[2].sourceConnectionID))
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aRetryComesFromANewConnectionID() throws {
        let server = try RetryServer(connectionIDGenerator: EchoingGenerator())
        let destinationConnectionID = server.randomConnectionID()

        // A client discards a Retry from the connection ID its Initial went to (RFC 9000 Section
        // 17.2.5.2).
        let retry = try server.retry(
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: server.randomConnectionID()
        )

        #expect(retry.sourceConnectionID != destinationConnectionID)
    }
}
