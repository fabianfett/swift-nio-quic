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
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import Testing

@testable import NIOQUIC

@Suite(.timeLimit(.minutes(1)))
struct ConnectionMigrationTests {
    @available(anyAppleOS 26, *)
    @Test
    func clientContinuesConnectionFromNewAddress() async throws {
        try await Self.withConnectedPeers { peers in
            // The client's socket "rebinds": its packets now come from a new port and its old address is gone.
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)

            // Only succeeds if the server moves the connection over to the client's new address.
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)
            // The server set up the request's stream before switching to the new path.
            #expect(peers.latestServerStreamChannel.withLockedValue { $0 }?.remoteAddress == newSocket.localAddress)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func clientMigratesToTwoNewAddresses() async throws {
        try await Self.withConnectedPeers { peers in
            for _ in 0..<2 {
                let newSocket = try await peers.makeClientSocket()
                try await peers.rebindClient(to: newSocket)

                #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
                #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test(.enabled { await Self.isDualStackAvailable() })
    func dualStackServerFollowsClientToTwoNewAddresses() async throws {
        try await Self.withConnectedPeers(serverHost: "::") { peers in
            for _ in 0..<2 {
                let newSocket = try await peers.makeClientSocket()
                try await peers.rebindClient(to: newSocket)

                #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
                let mappedAddress = try SocketAddress(
                    ipAddress: "::ffff:\(Self.host)",
                    port: newSocket.localAddress!.port!
                )
                #expect(peers.serverConnectionChannel.remoteAddress == mappedAddress)
            }
        }
    }

    /// What a client of the dual-stack server sends to `127.0.0.1` reaches that server, not a
    /// socket another test bound to `127.0.0.1` on the same port.
    @available(anyAppleOS 26, *)
    @Test(.enabled { await Self.isDualStackAvailable() })
    func dualStackServerSharesNoPortWithIPv4Sockets() async throws {
        let group = MultiThreadedEventLoopGroup.singleton
        // Some 2% of the ports in the ephemeral range: without the explicit port, about every 50th
        // dual-stack bind shares its port with one of them.
        var ipv4Sockets: [any Channel] = []
        defer {
            for socket in ipv4Sockets {
                socket.close(promise: nil)
            }
        }
        for _ in 0..<300 {
            ipv4Sockets.append(try await DatagramBootstrap(group: group).bind(host: Self.host, port: 0).get())
        }
        let ipv4Ports = Set(ipv4Sockets.map { $0.localAddress!.port! })

        var sharedPorts: [Int] = []
        for _ in 0..<400 {
            let server = try await Self.bindDualStack(on: group) { port in
                try await DatagramBootstrap(group: group).bind(host: "::", port: port).get()
            }
            let port = server.localAddress!.port!
            try await server.close()
            if ipv4Ports.contains(port) {
                sharedPorts.append(port)
            }
        }
        #expect(sharedPorts.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func clientMigratesBackToItsFirstAddress() async throws {
        try await Self.withConnectedPeers { peers in
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)

            try await peers.rebindClient(to: nil)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
        }
    }

    /// Anyone who sees the connection's traffic can send packets to it from any address.
    @available(anyAppleOS 26, *)
    @Test(arguments: [1, 2, 100])
    func serverIgnoresUnauthenticatedPacketsFromNewAddresses(addressCount: Int) async throws {
        try await Self.withConnectedPeers { peers in
            let packet = try await peers.unauthenticatedPacket()

            // Every new address makes the server set up a path, which SwiftNetwork then probes.
            for _ in 0..<addressCount {
                let spoofer = try await DatagramBootstrap(group: peers.eventLoopGroup)
                    .bind(host: Self.host, port: 0)
                    .get()
                try await spoofer.writeAndFlush(AddressedEnvelope(remoteAddress: peers.serverAddress, data: packet))
            }
            // Let SwiftNetwork's migration timer fire.
            try await Task.sleep(for: .seconds(1))

            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        }
    }

    /// The unauthenticated packet must not interrupt .
    @available(anyAppleOS 26, *)
    @Test
    func serverCloseReachesClientAfterUnauthenticatedPacket() async throws {
        try await Self.withConnectedPeers { peers in
            let spoofer = try await DatagramBootstrap(group: peers.eventLoopGroup).bind(host: Self.host, port: 0).get()
            try await spoofer.writeAndFlush(
                AddressedEnvelope(remoteAddress: peers.serverAddress, data: try await peers.unauthenticatedPacket())
            )
            try await Task.sleep(for: .milliseconds(100))

            let start = ContinuousClock.now
            try await peers.serverConnectionChannel.close()
            try await peers.clientConnectionChannel.closeFuture.get()
            // Without the server's CONNECTION_CLOSE the client only notices once its idle timeout fires.
            #expect(ContinuousClock.now - start < .seconds(5))
        }
    }

    private static let host = "127.0.0.1"

    /// Binds a dual-stack socket with `bind`, to a port that no socket on an IPv4 address has.
    ///
    /// macOS picks the ephemeral port of a socket bound to `[::]:0` without looking at the sockets
    /// bound to IPv4 addresses, so it can share its port with another test's socket on `127.0.0.1`,
    /// which then gets everything sent to `127.0.0.1` on that port: a client of this server would
    /// talk to the other test's server instead. A bind to an explicit port is checked against those
    /// sockets, and later ephemeral IPv4 binds skip the port, so this binds to the port a socket on
    /// `127.0.0.1` just got, once that socket is closed again.
    static func bindDualStack(
        on eventLoopGroup: any EventLoopGroup,
        _ bind: (_ port: Int) async throws -> any Channel
    ) async throws -> any Channel {
        var attempts = 0
        while true {
            let probe = try await DatagramBootstrap(group: eventLoopGroup).bind(host: Self.host, port: 0).get()
            let port = probe.localAddress!.port!
            try await probe.close()
            do {
                return try await bind(port)
            } catch let error as IOError where error.errnoCode == EADDRINUSE && attempts < 10 {
                // Another socket took the port in between.
                attempts += 1
            }
        }
    }

    /// Whether a socket can bind `::`, which NIO makes dual-stack.
    static func isDualStackAvailable() async -> Bool {
        let channel = try? await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "::", port: 0)
            .get()
        try? await channel?.close()
        return channel != nil
    }

    /// Connects a client to a server bound to `serverHost`, runs `body`, and tears both down.
    @available(anyAppleOS 26, *)
    private static func withConnectedPeers(
        serverHost: String = ConnectionMigrationTests.host,
        _ body: (ConnectedPeers) async throws -> Void
    ) async throws {
        let peers = try await ConnectedPeers.connect(serverHost: serverHost)
        do {
            try await body(peers)
        } catch {
            try? await peers.close()
            throw error
        }
        try await peers.close()
    }

    /// A client connected to a server over loopback, after one successful request.
    @available(anyAppleOS 26, *)
    private struct ConnectedPeers {
        var eventLoopGroup: MultiThreadedEventLoopGroup
        var serverChannel: any Channel
        var serverAddress: SocketAddress
        var serverConnectionChannel: any Channel
        var latestServerStreamChannel: NIOLockedValueBox<(any Channel)?>
        var clientChannel: any Channel
        var clientConnectionChannel: any Channel
        var streamCreator: QUICStreamCreator

        static func connect(serverHost: String) async throws -> ConnectedPeers {
            // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            do {
                return try await Self.connect(on: eventLoopGroup, serverHost: serverHost)
            } catch {
                // Shutting down the event loop group also closes the channels set up so far.
                try? await eventLoopGroup.shutdownGracefully()
                throw error
            }
        }

        private static func connect(
            on eventLoopGroup: MultiThreadedEventLoopGroup,
            serverHost: String
        ) async throws -> ConnectedPeers {
            let serverConnectionChannel = NIOLockedValueBox<(any Channel)?>(nil)
            let latestServerStreamChannel = NIOLockedValueBox<(any Channel)?>(nil)

            let bindServer = { (port: Int) in
                try await createServerChannel(
                    eventLoopGroup: eventLoopGroup,
                    host: serverHost,
                    port: port,
                    logger: Logger(label: "Server"),
                    inboundConnectionInitializer: { connectionChannel, _ in
                        serverConnectionChannel.withLockedValue { $0 = connectionChannel }
                        return connectionChannel.eventLoop.makeSucceededVoidFuture()
                    },
                    inboundStreamInitializer: { streamChannel in
                        latestServerStreamChannel.withLockedValue { $0 = streamChannel }
                        return streamChannel.eventLoop.makeCompletedFuture {
                            try streamChannel.pipeline.syncOperations.addHandler(TestServerHandler())
                        }
                    },
                    noMoreConnections: {}
                ).get()
            }
            let serverChannel =
                serverHost == "::"
                ? try await ConnectionMigrationTests.bindDualStack(on: eventLoopGroup, bindServer)
                : try await bindServer(0)
            let serverAddress = try SocketAddress(
                ipAddress: ConnectionMigrationTests.host,
                port: serverChannel.localAddress!.port!
            )

            let clientChannel = try await createClientChannel(
                eventLoopGroup: eventLoopGroup,
                host: ConnectionMigrationTests.host,
                port: 0,
                logger: Logger(label: "Client"),
                udpChannelInitializer: { channel in
                    try channel.pipeline.syncOperations.addHandler(AddressSwitchHandler())
                    try channel.pipeline.syncOperations.addHandler(ShortHeaderRecorder())
                }
            ).get()
            let (clientConnectionChannel, streamCreator) = try await clientChannel.pipeline.handler(
                type: QUICHandler<QUICStreamChannels>.self
            )
            .flatMap { quicHandler in
                quicHandler.createOutboundConnection(
                    serverName: "\(ConnectionMigrationTests.host):\(serverAddress.port!)",
                    remoteAddress: serverAddress,
                    connectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
                    inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() }
                )
            }.get()
            #expect(
                try await ConnectionMigrationTests.request(on: streamCreator) == ByteBuffer(string: "<b>Success</b>")
            )

            return ConnectedPeers(
                eventLoopGroup: eventLoopGroup,
                serverChannel: serverChannel,
                serverAddress: serverAddress,
                serverConnectionChannel: try #require(serverConnectionChannel.withLockedValue { $0 }),
                latestServerStreamChannel: latestServerStreamChannel,
                clientChannel: clientChannel,
                clientConnectionChannel: clientConnectionChannel,
                streamCreator: streamCreator
            )
        }

        /// A packet the server routes to the client's connection but can't decrypt: what anyone who
        /// sees the connection's traffic can send from any address.
        func unauthenticatedPacket() async throws -> ByteBuffer {
            let lastPacket = try await self.clientChannel.eventLoop.submit {
                try self.clientChannel.pipeline.syncOperations.handler(type: ShortHeaderRecorder.self).lastPacket
            }.get()
            let clientPacket = try #require(lastPacket)
            // A short header starts with one byte of flags, followed by the server's connection ID.
            let connectionID = try #require(
                clientPacket.getSlice(at: clientPacket.readerIndex + 1, length: Int(QUICConnectionID.randomIDLength))
            )
            var packet = ByteBuffer()
            packet.writeInteger(UInt8(0x40))  // Short header with the fixed bit set.
            packet.writeImmutableBuffer(connectionID)
            packet.writeRepeatingByte(0xAA, count: 41)
            return packet
        }

        /// A new socket for the client: what it receives goes into the client's pipeline while the client uses it.
        func makeClientSocket() async throws -> any Channel {
            let clientChannel = self.clientChannel
            return try await DatagramBootstrap(group: self.eventLoopGroup)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                    }
                }
                .bind(host: ConnectionMigrationTests.host, port: 0)
                .get()
        }

        /// Makes the client use `socket`, or its own socket for `nil`, as if its socket rebound to that port.
        func rebindClient(to socket: (any Channel)?) async throws {
            let clientChannel = self.clientChannel
            try await clientChannel.eventLoop.submit {
                let switchHandler = try clientChannel.pipeline.syncOperations.handler(type: AddressSwitchHandler.self)
                switchHandler.currentSocket = socket
            }.get()
        }

        func close() async throws {
            try await self.clientChannel.close()
            try await self.serverChannel.close()
            try await self.eventLoopGroup.shutdownGracefully()
        }
    }

    /// Sends a request on a new stream and returns the full response.
    @available(anyAppleOS 26, *)
    private static func request(on streamCreator: QUICStreamCreator) async throws -> ByteBuffer {
        let stream = try await streamCreator.createBidirectionalStream { initializer in
            initializer.channel.eventLoop.makeCompletedFuture {
                try NIOAsyncChannel(
                    wrappingChannelSynchronously: initializer.channel,
                    configuration: .init(
                        isOutboundHalfClosureEnabled: true,
                        inboundType: ByteBuffer.self,
                        outboundType: ByteBuffer.self
                    )
                )
            }
        }.get()
        return try await stream.executeThenClose { inbound, outbound in
            try await outbound.write(ByteBuffer(string: "GET /foo"))
            outbound.finish()
            var response = ByteBuffer()
            for try await chunk in inbound {
                response.writeImmutableBuffer(chunk)
            }
            return response
        }
    }
}

/// Sits in front of the client's `QUICHandler` and simulates its socket rebinding to other ports: datagrams
/// leave through `currentSocket`, or the client's own socket while it is `nil`. Datagrams arriving on any
/// other socket are dropped because that address is gone.
@available(anyAppleOS 26, *)
final class AddressSwitchHandler: ChannelDuplexHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    var currentSocket: (any Channel)?

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if self.currentSocket == nil {
            context.fireChannelRead(data)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        if let currentSocket = self.currentSocket {
            currentSocket.writeAndFlush(self.unwrapOutboundIn(data), promise: promise)
        } else {
            context.write(data, promise: promise)
        }
    }
}

/// Installed on a new socket of the client. While the client uses that socket, feeds what arrives there into
/// the client's pipeline just past the `AddressSwitchHandler`, so the client's `QUICHandler` receives it as before.
@available(anyAppleOS 26, *)
final class RelayHandler: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    private let clientChannel: any Channel

    init(clientChannel: any Channel) {
        self.clientChannel = clientChannel
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.switchContext(for: context.channel)?.fireChannelRead(data)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.switchContext(for: context.channel)?.fireChannelReadComplete()
    }

    // Both channels share the test's event loop, so the client's pipeline can be used synchronously.
    private func switchContext(for socket: any Channel) -> ChannelHandlerContext? {
        let operations = self.clientChannel.pipeline.syncOperations
        guard let switchHandler = try? operations.handler(type: AddressSwitchHandler.self),
            switchHandler.currentSocket === socket
        else {
            return nil
        }
        return try? operations.context(handlerType: AddressSwitchHandler.self)
    }
}

/// Sits in the client's pipeline and remembers the last short-header packet the client sent, which
/// carries the server's connection ID.
@available(anyAppleOS 26, *)
final class ShortHeaderRecorder: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    private(set) var lastPacket: ByteBuffer?

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let envelope = self.unwrapOutboundIn(data)
        // The most significant bit of the first byte is 0 for short headers.
        if let firstByte = envelope.data.getInteger(at: envelope.data.readerIndex, as: UInt8.self),
            firstByte & 0x80 == 0
        {
            self.lastPacket = envelope.data
        }
        context.write(data, promise: promise)
    }
}
