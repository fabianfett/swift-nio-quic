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
import Testing

@testable import NIOQUIC

/// The state machine connections of a handler share its timer queue, which one scheduled NIO task
/// serves.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineTimerTests {
    private static let connections = 16

    /// What a handler's timer queue holds.
    private struct Timers: Sendable, Equatable {
        /// Whether every connection's timer is armed in the handler's queue.
        var connectionsUseTheQueue: Bool
        var armedSlots: Int
        var isTaskScheduled: Bool
    }

    @available(anyAppleOS 26, *)
    private static func timers(
        of channel: any Channel,
        connections: [QUICStreamConnection<EchoConsumer>]
    ) async throws -> Timers {
        try await channel.eventLoop.submit {
            let queue = try channel.pipeline.syncOperations.handler(type: QUICHandler<EchoConsumer>.self)
                ._forTesting_timerQueue
            let connectionsUseTheQueue = connections.allSatisfy { connection in
                let stateMachine = (connection.channel as! QUICConnectionChannel<EchoConsumer>)
                    ._forTesting_stateMachineConnection!
                return stateMachine._forTesting_timerQueue === queue && stateMachine._forTesting_isTimerArmed
            }
            return Timers(
                connectionsUseTheQueue: connectionsUseTheQueue,
                armedSlots: queue._forTesting_armedSlots,
                isTaskScheduled: queue._forTesting_isTaskScheduled
            )
        }.get()
    }

    /// Waits until `condition` holds, for up to 20 seconds.
    private static func waitUntil(
        _ what: String,
        _ condition: () async throws -> Bool
    ) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(20))
        while try await !condition() {
            guard ContinuousClock.now < deadline else {
                throw TimedOut(description: what)
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// Many connections on a client handler and on a server handler arm their timers in their
    /// handler's queue, which has one task scheduled; a connection that closed leaves it.
    @available(anyAppleOS 26, *)
    @Test func theConnectionsOfAHandlerShareOneTimer() async throws {
        let serverConnections = NIOLockedValueBox<[QUICStreamConnection<EchoConsumer>]>([])
        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            client: EchoConsumer(),
            server: EchoConsumer(),
            onServerConnection: { connection in serverConnections.withLockedValue { $0.append(connection) } }
        ) { pair in
            let serverAddress = try #require(pair.serverChannel.localAddress)
            var clients = [pair.clientConnection]
            for _ in 1..<Self.connections {
                let client = try await pair.clientChannel.eventLoop.flatSubmit {
                    // '!' is fine: the pair's client channel has a handler.
                    try! pair.clientChannel.pipeline.syncOperations.handler(type: QUICHandler<EchoConsumer>.self)
                        .createOutboundConnection(
                            serverName: "127.0.0.1:\(serverAddress.port!)",
                            remoteAddress: serverAddress
                        )
                }.get()
                clients.append(client)
            }
            try await Self.waitUntil("the server accepts every connection") {
                serverConnections.withLockedValue { $0.count } == Self.connections
            }

            let everyConnection = Timers(
                connectionsUseTheQueue: true,
                armedSlots: Self.connections,
                isTaskScheduled: true
            )
            let clientTimers = try await Self.timers(of: pair.clientChannel, connections: clients)
            #expect(clientTimers == everyConnection)
            let servers = serverConnections.withLockedValue { $0 }
            let serverTimers = try await Self.timers(of: pair.serverChannel, connections: servers)
            #expect(serverTimers == everyConnection)

            // The closed connections leave the queues once they closed or drained (RFC 9000
            // Section 10.2), without their channels.
            for client in clients.dropFirst() {
                try await client.channel.close()
            }
            let oneConnection = Timers(connectionsUseTheQueue: true, armedSlots: 1, isTaskScheduled: true)
            try await Self.waitUntil("the closed connections leave the queues") {
                let client = try await Self.timers(of: pair.clientChannel, connections: [pair.clientConnection])
                let server = try await Self.timers(of: pair.serverChannel, connections: [])
                return client == oneConnection && server.armedSlots == 1
            }
        }
    }

    /// Once the UDP channel is gone, the handler's queue holds no connection and has no task
    /// scheduled, though the connections were closing.
    @available(anyAppleOS 26, *)
    @Test func aClosedUDPChannelReleasesTheTimers() async throws {
        let serverConnections = NIOLockedValueBox(0)
        let pair = try await makeConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            makeClientConsumer: { _ in EchoConsumer() },
            makeServerConsumer: { _ in
                serverConnections.withLockedValue { $0 += 1 }
                return EchoConsumer()
            }
        )
        do {
            let serverAddress = try #require(pair.serverChannel.localAddress)
            let clientChannel = pair.clientChannel
            let queue = try await clientChannel.eventLoop.flatSubmit {
                // '!' is fine: the pair's client channel has a handler.
                let handler = try! clientChannel.pipeline.syncOperations.handler(type: QUICHandler<EchoConsumer>.self)
                return handler.createOutboundConnection(
                    serverName: "127.0.0.1:\(serverAddress.port!)",
                    remoteAddress: serverAddress
                ).map { _ in NIOLoopBound(handler._forTesting_timerQueue, eventLoop: clientChannel.eventLoop) }
            }.get()
            try await Self.waitUntil("the server accepts both connections") {
                serverConnections.withLockedValue { $0 } == 2
            }
            let armed = try await clientChannel.eventLoop.submit { queue.value._forTesting_armedSlots }.get()
            #expect(armed == 2)

            try await clientChannel.close()
            let afterClose = try await clientChannel.eventLoop.submit {
                (queue.value._forTesting_armedSlots, queue.value._forTesting_isTaskScheduled)
            }.get()
            #expect(afterClose == (0, false))
            // Nothing arms a timer later.
            try await Task.sleep(for: .milliseconds(200))
            let later = try await clientChannel.eventLoop.submit {
                (queue.value._forTesting_armedSlots, queue.value._forTesting_isTaskScheduled)
            }.get()
            #expect(later == (0, false))
        } catch {
            try? await pair.clientChannel.close()
            try? await pair.serverChannel.close()
            throw error
        }
        try await pair.serverChannel.close()
    }
}
#endif
