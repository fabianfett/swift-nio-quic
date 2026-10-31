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
import NIOCore
import NIOEmbedded
import Testing
import XCTest

@testable import NIOQUIC

@Suite
struct QUICConnectionDriverTests {
    @available(anyAppleOS 26, *)
    private static var configuration: QUICConfiguration {
        QUICConfiguration.forClient(
            verificationConfiguration: .rawPublicKeys(publicKeyFilePath: "unused"),
            applicationProtocols: ["test"]
        )
    }

    /// A qlog runs on the state machine driver as well: the server's stream channel connection
    /// does.
    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aConnectionWithAQLogRunsOnTheStateMachine() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        var configuration = QUICConfiguration.forServer(
            serverName: "quic-test.local",
            authenticationConfiguration: .rawPublicKeys(
                publicKeyFilePath: XCTestCase.testPublicKeyPath,
                privateKeyFilePath: XCTestCase.testPrivateKeyPath
            ),
            applicationProtocols: []
        )
        configuration.qLogConfiguration = QLogConfiguration(path: directory.path)
        let channel = EmbeddedChannel()
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        let (handler, _) = try QUICHandler<QUICStreamChannels>.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: configuration,
            logger: Logger(label: "test"),
            inboundStreamChannelInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
        )
        handler.connectionDriver = .stateMachine
        let drivers = DriverRecorder()
        handler._forTesting_onStreamChannelsConnection = { drivers.drivers.append($0) }
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        var random = SystemRandomNumberGenerator()
        var packet = QUICPackets.initial(
            destinationID: .random(length: 8, using: &random),
            sourceID: .random(length: 8, using: &random),
            token: [],
            version: 1
        )
        packet += [UInt8](repeating: 0, count: 1200 - packet.count)
        channel.pipeline.fireChannelRead(
            AddressedEnvelope(remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 443), data: ByteBuffer(bytes: packet))
        )
        channel.pipeline.fireChannelReadComplete()

        #expect(drivers.drivers == [.stateMachine])
    }

    /// Handlers whose streams are stream channels default to the driver of those with stream
    /// consumers: the state machine, unless `NIOQUIC_DRIVER` says otherwise.
    @Test(arguments: [Role.client, .server])
    @available(anyAppleOS 26, *)
    func streamChannelsDefaultToTheConsumersDriver(role: Role) throws {
        try #require(QUICConnectionDriver._forTesting_streamChannelsDriver.withLock { $0 } == nil)
        #expect(QUICConnectionDriver.defaultForStreamChannels(for: role) == QUICConnectionDriver.default(for: role))

        let channel = EmbeddedChannel()
        defer { _ = try? channel.finish() }
        let handler = QUICHandler<QUICStreamChannels>(
            channel: channel,
            quicConfiguration: Self.configuration,
            asyncVerifier: nil,
            authenticator: nil,
            logger: Logger(label: "test"),
            inboundConnectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
            inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() },
            noMoreConnections: {}
        )
        #expect(handler.connectionDriver == QUICConnectionDriver.default(for: .client))
    }

    /// The state machine driver implements every configuration property: a new one must be
    /// added to ``QUICConnectionDriver/stateMachineProperties``.
    @Test
    @available(anyAppleOS 26, *)
    func everyConfigurationPropertyIsImplemented() {
        let properties = Set(Mirror(reflecting: Self.configuration).children.compactMap(\.label))

        #expect(properties == QUICConnectionDriver.stateMachineProperties)
    }
}

/// The drivers of the connections a handler made.
@available(anyAppleOS 26, *)
private final class DriverRecorder {
    var drivers: [QUICConnectionDriver] = []
}
