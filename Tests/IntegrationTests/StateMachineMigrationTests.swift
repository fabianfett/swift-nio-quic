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

/// Reads its streams to their end, counting what arrived so far, and reports each stream's total.
@available(anyAppleOS 26, *)
struct ProgressConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    let received: NIOLockedValueBox<Int>
    let finished: AsyncStream<Int>.Continuation

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let received = self.received
        let finished = self.finished
        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }
            visit.withStream { stream, _ in
                reading: while true {
                    var buffer = ByteBuffer()
                    switch stream.read(into: &buffer) {
                    case .read:
                        received.withLockedValue { $0 += buffer.readableBytes }
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        let total = received.withLockedValue {
                            $0 += buffer.readableBytes
                            return $0
                        }
                        finished.yield(total)
                        break reading
                    }
                }
            }
        }
    }
}

/// A state machine server follows a client whose NAT rebinds to a new address (RFC 9000 Section
/// 9.3), and doesn't let packets from other addresses grow its paths without limit.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineMigrationTests {
    private static let host = "127.0.0.1"

    @available(anyAppleOS 26, *)
    @Test(arguments: [
        Drivers(client: .stateMachine, server: .stateMachine),
        Drivers(client: .swiftNetwork, server: .stateMachine),
    ])
    func aTransferContinuesWhenTheClientRebinds(drivers: Drivers) async throws {
        // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        do {
            try await Self.transferWhileTheClientRebinds(drivers: drivers, eventLoopGroup: eventLoopGroup)
        } catch {
            try? await eventLoopGroup.shutdownGracefully()
            throw error
        }
        try await eventLoopGroup.shutdownGracefully()
    }

    @available(anyAppleOS 26, *)
    private static func transferWhileTheClientRebinds(
        drivers: Drivers,
        eventLoopGroup: MultiThreadedEventLoopGroup
    ) async throws {
        let chunkSize = 64 * 1024
        let chunkCount = 64
        let responseSize = chunkSize * chunkCount + "Success".utf8.count
        let received = NIOLockedValueBox(0)
        let (finished, continuation) = AsyncStream.makeStream(of: Int.self)
        var responses = finished.makeAsyncIterator()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<StreamingConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<StreamingConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }

        try await withConsumerPair(
            drivers: drivers,
            eventLoopGroup: eventLoopGroup,
            // The server's packets to the old address are lost, so the client only sends again,
            // from its new address, once a keepalive is due.
            configureClient: { $0.keepAliveInterval = .milliseconds(100) },
            configureClientPipeline: { try $0.addHandler(AddressSwitchHandler()) },
            client: ProgressConsumer(received: received, finished: continuation),
            server: StreamingConsumer(chunkSize: chunkSize, chunkCount: chunkCount),
            onServerConnection: captureConnection
        ) { pair in
            _ = try await pair.openClientStream(state: (), writing: ByteBuffer(string: "GET /quic"), fin: true)
            // Rebind once the response is under way.
            while received.withLockedValue({ $0 }) < 256 * 1024 {
                try await Task.sleep(for: .milliseconds(1))
            }
            try #require(received.withLockedValue { $0 } < responseSize, "the response arrived before the rebinding")

            // The client's socket "rebinds": its packets now come from a new port and its old address is gone.
            let clientChannel = pair.clientChannel
            let newSocket = try await DatagramBootstrap(group: eventLoopGroup)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                    }
                }
                .bind(host: Self.host, port: 0)
                .get()
            defer { newSocket.close(promise: nil) }
            try await clientChannel.eventLoop.submit {
                try clientChannel.pipeline.syncOperations.handler(type: AddressSwitchHandler.self).currentSocket =
                    newSocket
            }.get()

            // Only completes if the server moves the connection over to the client's new address.
            #expect(await responses.next() == responseSize)
            let server = try #require(serverConnection.withLockedValue { $0 })
            #expect(server.channel.remoteAddress == newSocket.localAddress)
        }
    }

    /// A client that moves to a new address sends from there to a spare connection ID of the
    /// server (RFC 9000 Sections 9.2 and 9.5), which the server's handler routes to the
    /// connection, and the server follows it. The core client migrates once it validated the new
    /// path, which it can only do if the server doesn't disable active migration.
    @available(anyAppleOS 26, *)
    @Test
    func aClientMigratesToANewAddressWithANewConnectionID() async throws {
        // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        do {
            try await Self.migrateTheClient(eventLoopGroup: eventLoopGroup)
        } catch {
            try? await eventLoopGroup.shutdownGracefully()
            throw error
        }
        try await eventLoopGroup.shutdownGracefully()
    }

    @available(anyAppleOS 26, *)
    private static func migrateTheClient(eventLoopGroup: MultiThreadedEventLoopGroup) async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }
        func roundTrip(_ pair: ConsumerPair<RecordConsumer, EchoConsumer>) async throws {
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /"), fin: true)
            let visits = await recording.visits { $0.handle == handle && $0.events.contains(.closed) }
            #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "GET /"))
        }

        let switchAddresses: @Sendable (ChannelPipeline.SynchronousOperations) throws -> Void = {
            try $0.addHandler(AddressSwitchHandler())
        }

        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            eventLoopGroup: eventLoopGroup,
            configureClientPipeline: switchAddresses,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            try await roundTrip(pair)
            let server = try #require(serverConnection.withLockedValue { $0 }).channel
            let serverConnectionIDs = try await server.eventLoop.submit {
                (server as! QUICConnectionChannel<EchoConsumer>)._forTesting_stateMachineConnection!
                    ._forTesting_localConnectionIDs
            }.get()
            let client = pair.clientConnection.channel
            let clientConnection = { @Sendable in
                (client as! QUICConnectionChannel<RecordConsumer>)._forTesting_stateMachineConnection!
            }
            let handshakeID = try await client.eventLoop.submit { clientConnection()._forTesting_peerConnectionID }
                .get()
            #expect(handshakeID == serverConnectionIDs.first)

            // The client moves to a new socket, and its old address is gone.
            let clientChannel = pair.clientChannel
            let newSocket = try await DatagramBootstrap(group: eventLoopGroup)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                    }
                }
                .bind(host: Self.host, port: 0)
                .get()
            defer { newSocket.close(promise: nil) }
            let migrated = try await clientChannel.eventLoop.submit {
                try clientChannel.pipeline.syncOperations.handler(type: AddressSwitchHandler.self).currentSocket =
                    newSocket
                return clientConnection()._forTesting_migrateToANewPath()
            }.get()
            try #require(migrated, "the client can't migrate")

            // The client sends to a spare once it validated the new path.
            var connectionID = handshakeID
            let deadline = ContinuousClock.now + .seconds(10)
            while connectionID == handshakeID, ContinuousClock.now < deadline {
                try await Task.sleep(for: .milliseconds(1))
                connectionID = try await client.eventLoop.submit { clientConnection()._forTesting_peerConnectionID }
                    .get()
            }
            try #require(connectionID != handshakeID, "the client didn't validate the new path")
            let spare = try #require(connectionID)
            #expect(serverConnectionIDs.dropFirst().contains(spare))

            // Only completes if the server routes the spare to the connection.
            try await roundTrip(pair)
            #expect(server.remoteAddress == newSocket.localAddress)
        }
    }

    /// Anyone who sees the connection's traffic can send packets to it from any address.
    @available(anyAppleOS 26, *)
    @Test
    func packetsFromManyAddressesDontGrowTheServersPaths() async throws {
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            configureClientPipeline: { try $0.addHandler(ShortHeaderRecorder()) },
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
            _ = await recording.visits(untilClosedStreams: 1)

            // A packet the server routes to the connection but can't decrypt.
            let clientChannel = pair.clientChannel
            let lastPacket = try await clientChannel.eventLoop.submit {
                try clientChannel.pipeline.syncOperations.handler(type: ShortHeaderRecorder.self).lastPacket
            }.get()
            let clientPacket = try #require(lastPacket)
            let connectionID = try #require(
                clientPacket.getSlice(at: clientPacket.readerIndex + 1, length: Int(QUICConnectionID.randomIDLength))
            )
            var packet = ByteBuffer()
            packet.writeInteger(UInt8(0x40))  // Short header with the fixed bit set.
            packet.writeImmutableBuffer(connectionID)
            packet.writeRepeatingByte(0xAA, count: 41)

            let server = try #require(serverConnection.withLockedValue { $0 })
            let serverAddress = try SocketAddress(ipAddress: Self.host, port: pair.serverChannel.localAddress!.port!)
            for _ in 0..<20 {
                let spoofer = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
                    .bind(host: Self.host, port: 0)
                    .get()
                try await spoofer.writeAndFlush(AddressedEnvelope(remoteAddress: serverAddress, data: packet))
                try await spoofer.close()
            }
            // Wait for the server to have processed the packets: one more request after them.
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
            _ = await recording.visits(untilClosedStreams: 1)

            let pathCount = try await server.channel.eventLoop.submit {
                (server.channel as! QUICConnectionChannel<EchoConsumer>)._forTesting_stateMachineConnection!
                    ._forTesting_pathCount
            }.get()
            #expect(pathCount <= StateMachineQUICConnection<EchoConsumer>.maximumPaths)
            #expect(server.channel.remoteAddress == clientChannel.localAddress)
        }
    }
}
#endif
