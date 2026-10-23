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
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

@available(anyAppleOS 26, *)
struct CertificateTestDrivers: Sendable, CustomTestStringConvertible {
    var client: QUICConnectionDriver
    var server: QUICConnectionDriver

    static let all: [CertificateTestDrivers] = [
        CertificateTestDrivers(client: .swiftNetwork, server: .swiftNetwork),
        CertificateTestDrivers(client: .stateMachine, server: .stateMachine),
        CertificateTestDrivers(client: .stateMachine, server: .swiftNetwork),
        CertificateTestDrivers(client: .swiftNetwork, server: .stateMachine),
    ]

    var testDescription: String {
        "client: \(self.client), server: \(self.server)"
    }
}

/// Echoes every stream back once it has been read to its end.
@available(anyAppleOS 26, *)
private struct EchoServer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, buffer in
                reading: while true {
                    switch stream.read(into: &buffer) {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        stream.write(buffer)
                        try? stream.flush(fin: true)
                        break reading
                    }
                }
            }
        }
    }
}

/// Reports each stream's whole response.
@available(anyAppleOS 26, *)
private struct ResponseCollector: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    let responses: AsyncStream<ByteBuffer>.Continuation

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let responses = self.responses
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, buffer in
                reading: while true {
                    switch stream.read(into: &buffer) {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        responses.yield(buffer)
                        break reading
                    }
                }
            }
        }
    }
}

/// X.509 authentication for connections that serve a stream consumer, on every driver: the client
/// verifies the server's chain asynchronously, through an ``AsyncVerifier``.
@Suite(.timeLimit(.minutes(1)))
struct StreamConsumerCertificateTests {
    @available(anyAppleOS 26, *)
    private static func serverChannel(
        certificates: TestCAFilePaths,
        serverName: String,
        driver: QUICConnectionDriver
    ) async throws -> any Channel {
        let configuration = QUICConfiguration.forServer(
            serverName: serverName,
            authenticationConfiguration: .x509Certificates(
                certificateChainFilePath: certificates.serverCertFilePath,
                privateKeyFilePath: certificates.serverPrivateKeyFilePath
            ),
            applicationProtocols: ["echo"]
        )
        return try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<EchoServer>(
                    channel: channel,
                    quicConfiguration: configuration,
                    asyncVerifier: nil,
                    authenticator: try Authenticator(
                        certificateFilePath: certificates.serverCertFilePath,
                        privateKeyFilePath: certificates.serverPrivateKeyFilePath
                    ),
                    logger: Logger(label: "Server"),
                    makeConsumer: { _ in EchoServer() }
                )
                handler.connectionDriver = driver
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
    }

    /// Connects with `trustRoots` as the only trust anchors, sends `request` and returns the
    /// response, or `nil` if the connection closed without one.
    @available(anyAppleOS 26, *)
    private static func echo(
        _ request: String,
        to server: any Channel,
        serverName: String,
        trustRoots: TestCAFilePaths,
        driver: QUICConnectionDriver
    ) async throws -> String? {
        let (responses, continuation) = AsyncStream<ByteBuffer>.makeStream()
        let configuration = QUICConfiguration.forClient(
            verificationConfiguration: .x509Certificates(trustRootsFilePath: trustRoots.trustStoreFilePath),
            applicationProtocols: ["echo"]
        )
        let client = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<ResponseCollector>(
                    channel: channel,
                    quicConfiguration: configuration,
                    asyncVerifier: try AsyncVerifier(
                        trustRootsPath: trustRoots.trustStoreFilePath,
                        eventLoop: channel.eventLoop
                    ),
                    authenticator: nil,
                    logger: Logger(label: "Client"),
                    makeConsumer: { _ in ResponseCollector(responses: continuation) }
                )
                handler.connectionDriver = driver
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        defer { client.close(promise: nil) }

        let connection = try await client.pipeline.handler(type: QUICHandler<ResponseCollector>.self)
            .flatMap { $0.createOutboundConnection(serverName: serverName, remoteAddress: server.localAddress!) }
            .get()
        #expect(connection.driver == driver)
        // A failed handshake closes the connection channel; end the response stream then.
        connection.channel.closeFuture.whenComplete { _ in continuation.finish() }
        _ = try await connection.withStreams { streams in
            let handle = try streams.open(.clientInitiatedBidirectional, state: ByteBuffer())
            try streams.withStream(handle: handle) { stream, _ in
                stream.write(ByteBuffer(string: request))
                try stream.flush(fin: true)
            }
        }.get()

        for await response in responses {
            return String(buffer: response)
        }
        return nil
    }

    @Test(arguments: CertificateTestDrivers.all)
    @available(anyAppleOS 26, *)
    func aVerifiedServerEchoes(drivers: CertificateTestDrivers) async throws {
        let certificates = try TestCertificates()
        // Each case writes its own files: the cases run in parallel.
        let paths = try certificates.writeToTemp(fileTag: "aVerifiedServerEchoes-\(UUID())")
        let server = try await Self.serverChannel(
            certificates: paths,
            serverName: certificates.leafName,
            driver: drivers.server
        )
        let response = try await Self.echo(
            "ping",
            to: server,
            serverName: certificates.leafName,
            trustRoots: paths,
            driver: drivers.client
        )
        try await server.close()
        #expect(response == "ping")
    }

    @Test(arguments: CertificateTestDrivers.all)
    @available(anyAppleOS 26, *)
    func aServerWithAnUntrustedChainIsRejected(drivers: CertificateTestDrivers) async throws {
        let certificates = try TestCertificates()
        let paths = try certificates.writeToTemp(fileTag: "aServerWithAnUntrustedChainIsRejected-\(UUID())")
        let otherRoots = try TestCertificates().writeToTemp(fileTag: "aServerWithAnUntrustedChainIsRejected-\(UUID())")
        let server = try await Self.serverChannel(
            certificates: paths,
            serverName: certificates.leafName,
            driver: drivers.server
        )
        let response = try? await Self.echo(
            "ping",
            to: server,
            serverName: certificates.leafName,
            trustRoots: otherRoots,
            driver: drivers.client
        )
        try await server.close()
        #expect(response == nil)
    }
}
