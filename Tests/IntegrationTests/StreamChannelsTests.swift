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
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

/// Stream channels over a real connection.
@Suite(.timeLimit(.minutes(1)))
struct StreamChannelsTests {
    /// A connection channel that closes gracefully tells its stream channels first, so that they
    /// can finish.
    @available(anyAppleOS 26, *)
    @Test
    func aGracefulCloseQuiescesTheStreams() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let host = "127.0.0.1"
        let serverConnection = eventLoopGroup.any().makePromise(of: (any Channel).self)
        let serverStreamQuiesced = eventLoopGroup.any().makePromise(of: Void.self)
        let serverStreamRead = eventLoopGroup.any().makePromise(of: Void.self)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Server"),
            inboundConnectionInitializer: { connectionChannel, _ in
                serverConnection.succeed(connectionChannel)
                return connectionChannel.eventLoop.makeSucceededVoidFuture()
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(
                        QuiesceRecorder(read: serverStreamRead, quiesced: serverStreamQuiesced)
                    )
                }
            },
            noMoreConnections: {}
        ).get()
        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Client")
        ).get()

        let (_, streamCreator) = try await connectOutbound(
            clientChannel,
            host: host,
            port: serverChannel.localAddress!.port!
        )
        let stream = try await streamCreator.createBidirectionalStream { streamInitializer in
            streamInitializer.channel.eventLoop.makeSucceededFuture(streamInitializer.channel)
        }.get()
        try await stream.writeAndFlush(ByteBuffer(string: "hello"))
        try await serverStreamRead.futureResult.get()

        // Fail rather than hang if the event never comes.
        serverStreamQuiesced.futureResult.eventLoop.scheduleTask(in: .seconds(10)) {
            serverStreamQuiesced.fail(ChannelError.connectTimeout(.seconds(10)))
        }
        try await serverConnection.futureResult.get().close()
        try await serverStreamQuiesced.futureResult.get()

        try await clientChannel.close()
        try await serverChannel.close()
    }
}

/// Succeeds its promises on the first read and on `ChannelShouldQuiesceEvent`.
@available(anyAppleOS 26, *)
private final class QuiesceRecorder: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let read: EventLoopPromise<Void>
    private let quiesced: EventLoopPromise<Void>

    init(read: EventLoopPromise<Void>, quiesced: EventLoopPromise<Void>) {
        self.read = read
        self.quiesced = quiesced
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.read.succeed()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is ChannelShouldQuiesceEvent {
            self.quiesced.succeed()
        }
        context.fireUserInboundEventTriggered(event)
    }
}
