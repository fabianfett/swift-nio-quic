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

import NIOConcurrencyHelpers
import NIOCore
import Testing

@testable import NIOQUIC

/// Configuration options of connections that serve a stream consumer, on every driver pairing.
/// `ConsumerPair.close` checks that each connection ran on the driver it was meant for, so these
/// also check that the state machine driver accepts the configurations.
@Suite(.timeLimit(.minutes(1)))
struct ConsumerConnectionConfigurationTests {
    /// Adds a handler that hands every datagram the connection receives to `onDatagram`, which
    /// runs on the connection's event loop.
    @available(anyAppleOS 26, *)
    private static func onDatagram<Consumer: QUICStreamConsumer>(
        of connection: QUICStreamConnection<Consumer>,
        _ onDatagram: @escaping @Sendable (_ datagram: ByteBuffer, _ channel: any Channel) -> Void
    ) {
        let channel = connection.channel
        channel.eventLoop.assertInEventLoop()
        try! channel.pipeline.syncOperations.addHandler(
            DatagramCapture(onDatagram: { datagram in onDatagram(datagram, channel) })
        )
    }

    @available(anyAppleOS 26, *)
    private static func addClientDatagramCapture<Client, Server>(
        to pair: ConsumerPair<Client, Server>,
        _ onDatagram: @escaping @Sendable (_ datagram: ByteBuffer) -> Void
    ) async throws {
        let channel = pair.clientConnection.channel
        try await channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(DatagramCapture(onDatagram: onDatagram))
        }.get()
    }

    /// Datagrams (RFC 9221) reach the peer, and the peer's answers come back.
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testDatagramsAreEchoed(drivers: Drivers) async throws {
        let echoed = makePromise(of: ByteBuffer.self, timeout: .seconds(5))

        try await withConsumerPair(
            drivers: drivers,
            client: EchoConsumer(),
            server: EchoConsumer(),
            onServerConnection: { connection in
                Self.onDatagram(of: connection) { datagram, channel in
                    channel.writeAndFlush(datagram, promise: nil)
                }
            },
            { pair in
                try await Self.addClientDatagramCapture(to: pair) { echoed.succeed($0) }
                try await pair.clientConnection.channel.writeAndFlush(ByteBuffer(string: "ping"))
                #expect(try await echoed.futureResult.get() == ByteBuffer(string: "ping"))
            }
        )
    }

    /// A peer which advertised no `max_datagram_frame_size` gets no datagrams: the write fails.
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testDatagramsToAPeerWithoutDatagramSupportFail(drivers: Drivers) async throws {
        try await withConsumerPair(
            drivers: drivers,
            configureServer: { $0.maxDatagramFrameSize = 0 },
            client: EchoConsumer(),
            server: EchoConsumer(),
            { pair in
                _ = await #expect(throws: QUICError.peerDoesNotAcceptDatagrams) {
                    try await pair.clientConnection.channel.writeAndFlush(ByteBuffer(string: "ping"))
                }
            }
        )
    }

    /// A datagram larger than the peer's `max_datagram_frame_size` fails; smaller ones still work.
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testDatagramsLargerThanThePeerAcceptsFail(drivers: Drivers) async throws {
        let echoed = makePromise(of: ByteBuffer.self, timeout: .seconds(5))

        try await withConsumerPair(
            drivers: drivers,
            configureServer: { $0.maxDatagramFrameSize = 256 },
            client: EchoConsumer(),
            server: EchoConsumer(),
            onServerConnection: { connection in
                Self.onDatagram(of: connection) { datagram, channel in
                    channel.writeAndFlush(datagram, promise: nil)
                }
            },
            { pair in
                try await Self.addClientDatagramCapture(to: pair) { echoed.succeed($0) }
                await #expect(throws: QUICError.datagramTooLarge) {
                    try await pair.clientConnection.channel.writeAndFlush(ByteBuffer(repeating: 0x78, count: 800))
                }
                try await pair.clientConnection.channel.writeAndFlush(ByteBuffer(string: "small"))
                #expect(try await echoed.futureResult.get() == ByteBuffer(string: "small"))
            }
        )
    }

    /// No packet is larger than the configured packet size, so a datagram that needs a larger
    /// packet isn't sent; a server matching the client's Initial uses the client's size.
    @available(anyAppleOS 26, *)
    @Test(
        arguments: Drivers.all,
        [
            (InitialPacketSize.fixed(1200), 1350, [6]),
            (InitialPacketSize.fixed(1350), 1350, [1250, 6]),
            (InitialPacketSize.matchingClientInitial(upTo: 1500), 1350, [1250, 6]),
            (InitialPacketSize.matchingClientInitial(upTo: 1500), 1200, [6]),
        ]
    )
    func testPacketsAreNoLargerThanTheInitialPacketSize(
        drivers: Drivers,
        sizes: (server: InitialPacketSize, client: Int, expected: [Int])
    ) async throws {
        let marker = ByteBuffer(string: "marker")
        let markerReceived = makePromise(of: Void.self, timeout: .seconds(5))
        let received = NIOLockedValueBox<[Int]>([])

        try await withConsumerPair(
            drivers: drivers,
            configureClient: { $0.initialPacketSize = .fixed(sizes.client) },
            configureServer: { $0.initialPacketSize = sizes.server },
            client: EchoConsumer(),
            server: EchoConsumer(),
            onServerConnection: { connection in
                Self.onDatagram(of: connection) { _, channel in
                    // A datagram that needs a larger packet fails; the marker shows that
                    // everything that fits was delivered.
                    channel.write(ByteBuffer(repeating: 0x78, count: 1250), promise: nil)
                    channel.writeAndFlush(marker, promise: nil)
                }
            },
            { pair in
                try await Self.addClientDatagramCapture(to: pair) { datagram in
                    received.withLockedValue { $0.append(datagram.readableBytes) }
                    if datagram == marker {
                        markerReceived.succeed()
                    }
                }
                try await pair.clientConnection.channel.writeAndFlush(ByteBuffer(string: "go"))
                try await markerReceived.futureResult.get()
                #expect(received.withLockedValue { $0 } == sizes.expected)
            }
        )
    }

    /// Keepalives hold an otherwise idle connection open past its idle timeout.
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testKeepalivesHoldAnIdleConnectionOpen(drivers: Drivers) async throws {
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            maxIdleTimeout: .seconds(2),
            // The SwiftNetwork driver counts keepalive intervals in whole seconds.
            configureClient: { $0.keepAliveInterval = .seconds(1) },
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            { pair in
                try await Task.sleep(for: .seconds(5))
                #expect(pair.clientConnection.channel.isActive)

                let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /"), fin: true)
                let visits = await recording.visits { $0.handle == handle && $0.events.contains(.closed) }
                #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "GET /"))
                #expect(visits.last?.closeError == nil)
            }
        )
    }

    /// A client forcing Version Negotiation (RFC 9000 Section 6) still connects.
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testAClientForcingVersionNegotiationConnects(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let versionNegotiations = NIOLockedValueBox(0)

        try await withConsumerPair(
            drivers: drivers,
            configureClient: { $0.forceVersionNegotiation = true },
            inspectClientDatagram: { datagram in
                // A long header with version 0 (RFC 9000 Section 17.2.1).
                if let first = datagram.getInteger(at: datagram.readerIndex, as: UInt8.self), first & 0x80 != 0,
                    datagram.getInteger(at: datagram.readerIndex + 1, as: UInt32.self) == 0
                {
                    versionNegotiations.withLockedValue { $0 += 1 }
                }
            },
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            { pair in
                let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /"), fin: true)
                let visits = await recording.visits { $0.handle == handle && $0.events.contains(.closed) }
                #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "GET /"))
            }
        )
        // RFC 9000 Section 6.1: a server answers an Initial in a version it doesn't support with
        // Version Negotiation.
        #expect(versionNegotiations.withLockedValue { $0 } > 0, "the server sent no Version Negotiation")
    }
}
