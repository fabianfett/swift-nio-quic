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
import Testing

@testable import NIOQUIC

/// Records the connection ID events a connection channel fires, in order, and passes them on.
@available(anyAppleOS 26, *)
final class ConnectionIDEventRecorder: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer

    enum Event: Equatable, Sendable {
        case associated(QUICConnectionID)
        case retired(QUICConnectionID)

        var isAssociated: Bool {
            if case .associated = self { return true }
            return false
        }
    }

    private let continuation: AsyncStream<Event>.Continuation
    private let stream: AsyncStream<Event>

    init() {
        (self.stream, self.continuation) = AsyncStream<Event>.makeStream()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case let associated as QUICSCIDAssociatedEvent:
            self.continuation.yield(.associated(associated.scid))
        case let retired as QUICSCIDRetiredEvent:
            self.continuation.yield(.retired(retired.scid))
        default:
            ()
        }
        context.fireUserInboundEventTriggered(event)
    }

    /// The events recorded since the last call, up to and including the first one matching
    /// `predicate`.
    ///
    /// Waits indefinitely: a test which never sees its event is ended by the suite's time limit.
    func events(until predicate: @Sendable (_ event: Event) -> Bool) async -> [Event] {
        var recorded: [Event] = []
        for await event in self.stream {
            recorded.append(event)
            if predicate(event) {
                break
            }
        }
        return recorded
    }
}

/// Connection IDs on state machine connections (RFC 9000 Section 5.1): each side issues spares
/// after the handshake, which the handler routes, replaces those the peer retires, and serves
/// the connection ID requests of the connection channel.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineConnectionIDTests {
    /// The handshake's connection ID and the eight spares the connection issues.
    private static let issuedConnectionIDs = 1 + 8

    @available(anyAppleOS 26, *)
    private static func stateMachineConnection<Consumer: QUICStreamConsumer>(
        _ channel: any Channel,
        as consumer: Consumer.Type
    ) -> StateMachineQUICConnection<Consumer> {
        (channel as! QUICConnectionChannel<Consumer>)._forTesting_stateMachineConnection!
    }

    /// The connection's own connection IDs that route to it, oldest first.
    @available(anyAppleOS 26, *)
    private static func localConnectionIDs<Consumer: QUICStreamConsumer>(
        of channel: any Channel,
        as consumer: Consumer.Type
    ) async throws -> [QUICConnectionID] {
        try await channel.eventLoop.submit {
            Self.stateMachineConnection(channel, as: consumer)._forTesting_localConnectionIDs
        }.get()
    }

    /// The retired connection IDs the connection refuses to issue again, oldest first.
    @available(anyAppleOS 26, *)
    private static func retiredConnectionIDs<Consumer: QUICStreamConsumer>(
        of channel: any Channel,
        as consumer: Consumer.Type
    ) async throws -> [QUICConnectionID] {
        try await channel.eventLoop.submit {
            Self.stateMachineConnection(channel, as: consumer)._forTesting_retiredConnectionIDs
        }.get()
    }

    /// The peer's connection ID the connection sends to.
    @available(anyAppleOS 26, *)
    private static func peerConnectionID<Consumer: QUICStreamConsumer>(
        of channel: any Channel,
        as consumer: Consumer.Type
    ) async throws -> QUICConnectionID? {
        try await channel.eventLoop.submit {
            Self.stateMachineConnection(channel, as: consumer)._forTesting_peerConnectionID
        }.get()
    }

    @available(anyAppleOS 26, *)
    private static func addRecorder(to channel: any Channel) async throws -> ConnectionIDEventRecorder {
        let recorder = ConnectionIDEventRecorder()
        try await channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(recorder)
        }.get()
        return recorder
    }

    /// A request echoed back, which only completes if the packets of both sides reach the other.
    @available(anyAppleOS 26, *)
    private static func roundTrip(
        _ pair: ConsumerPair<RecordConsumer, EchoConsumer>,
        recording: StreamRecording
    ) async throws {
        let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /"), fin: true)
        let visits = await recording.visits { $0.handle == handle && $0.events.contains(.closed) }
        #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "GET /"))
        #expect(visits.last?.closeError == nil)
    }

    @available(anyAppleOS 26, *)
    private static func randomConnectionID() -> QUICConnectionID {
        var generator: any RandomNumberGenerator = SystemRandomNumberGenerator()
        return QUICConnectionID.random(using: &generator)
    }

    /// Both sides issue spares after the handshake. A client that retires the server's connection
    /// ID it sends to switches to a spare, which the server's handler routes to the connection;
    /// the server issues a new one for the retired one.
    @available(anyAppleOS 26, *)
    @Test
    func aClientRetiringTheServersConnectionIDSwitchesToASpare() async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }

        try await withConsumerPair(
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            try await Self.roundTrip(pair, recording: recording)
            let server = try #require(serverConnection.withLockedValue { $0 }).channel
            let client = pair.clientConnection.channel

            let serverIDs = try await Self.localConnectionIDs(of: server, as: EchoConsumer.self)
            let clientIDs = try await Self.localConnectionIDs(of: client, as: RecordConsumer.self)
            #expect(serverIDs.count == Self.issuedConnectionIDs)
            #expect(clientIDs.count == Self.issuedConnectionIDs)
            #expect(Set(serverIDs).count == serverIDs.count)

            let serverEvents = try await Self.addRecorder(to: server)

            // The client sends to the server's connection ID of the handshake.
            let inUse = try #require(try await Self.peerConnectionID(of: client, as: RecordConsumer.self))
            #expect(inUse == serverIDs.first)
            try await client.triggerUserOutboundEvent(QUICRequestRetireDCIDEvent(dcid: inUse)).get()
            let spare = try #require(try await Self.peerConnectionID(of: client, as: RecordConsumer.self))
            #expect(spare != inUse)
            #expect(serverIDs.contains(spare))

            // Only completes if the server's handler routes the spare to the connection.
            try await Self.roundTrip(pair, recording: recording)

            let events = await serverEvents.events { $0.isAssociated }
            #expect(events.count == 2)
            #expect(events.first == .retired(inUse))
            let replacement = try #require(events.last)
            let afterwards = try await Self.localConnectionIDs(of: server, as: EchoConsumer.self)
            #expect(afterwards.count == Self.issuedConnectionIDs)
            #expect(!afterwards.contains(inUse))
            #expect(afterwards.contains(spare))
            let newest = try #require(afterwards.last)
            #expect(replacement == .associated(newest))
            #expect(try await Self.retiredConnectionIDs(of: server, as: EchoConsumer.self) == [inUse])
        }
    }

    /// A connection ID the connection channel asks for is issued to the peer and routed; once the
    /// peer retires it, it is no longer, and another one replaces it.
    @available(anyAppleOS 26, *)
    @Test
    func aRequestedConnectionIDIsIssuedAndRetired() async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }
        let requested = Self.randomConnectionID()

        try await withConsumerPair(
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            try await Self.roundTrip(pair, recording: recording)
            let server = try #require(serverConnection.withLockedValue { $0 }).channel
            let client = pair.clientConnection.channel
            let clientEvents = try await Self.addRecorder(to: client)

            try await client.triggerUserOutboundEvent(QUICRequestAssociateSCIDEvent(scid: requested)).get()
            #expect(await clientEvents.events { $0.isAssociated } == [.associated(requested)])

            // The server learns of it with the packets of the request.
            try await Self.roundTrip(pair, recording: recording)
            try await server.triggerUserOutboundEvent(QUICRequestRetireDCIDEvent(dcid: requested)).get()

            let events = await clientEvents.events { $0.isAssociated }
            #expect(events.count == 2)
            #expect(events.first == .retired(requested))
            try await Self.roundTrip(pair, recording: recording)

            let afterwards = try await Self.localConnectionIDs(of: client, as: RecordConsumer.self)
            #expect(afterwards.count == Self.issuedConnectionIDs + 1)
            #expect(!afterwards.contains(requested))
            #expect(try await Self.retiredConnectionIDs(of: client, as: RecordConsumer.self) == [requested])
        }
    }

    /// The connection refuses a connection ID it issued before: issuing it again is a protocol
    /// violation (RFC 9000 Section 5.1.1), which closes the connection on both sides.
    @available(anyAppleOS 26, *)
    @Test
    func issuingARetiredConnectionIDAgainClosesTheConnection() async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }
        let reissued = Self.randomConnectionID()

        try await withConsumerPair(
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            try await Self.roundTrip(pair, recording: recording)
            let server = try #require(serverConnection.withLockedValue { $0 }).channel
            let client = pair.clientConnection.channel

            try await client.triggerUserOutboundEvent(_QUICForTestingPoisonRetiredSCIDEvent(scid: reissued)).get()
            await #expect(throws: QUICError.failedToAssociateConnectionID) {
                try await client.triggerUserOutboundEvent(QUICRequestAssociateSCIDEvent(scid: reissued)).get()
            }

            try await client.closeFuture.get()
            try await server.closeFuture.get()
        }
    }

    /// The peer retiring the last connection ID that routes to the connection doesn't make it
    /// unreachable: the ID keeps routing until another one does, and is retired then.
    @available(anyAppleOS 26, *)
    @Test
    func theLastConnectionIDIsRetiredOnceAnotherOneRoutes() async throws {
        let recording = StreamRecording()
        let first = Self.randomConnectionID()
        let second = Self.randomConnectionID()

        try await withConsumerPair(
            client: RecordConsumer(recording: recording),
            server: EchoConsumer()
        ) { pair in
            try await Self.roundTrip(pair, recording: recording)
            let client = pair.clientConnection.channel
            let initial = try await Self.localConnectionIDs(of: client, as: RecordConsumer.self)
            #expect(initial.count == Self.issuedConnectionIDs)

            let clientEvents = ConnectionIDEventRecorder()
            try await client.eventLoop.submit {
                // Recorded before the forwarder, which consumes the events.
                try client.pipeline.syncOperations.addHandler(clientEvents)
                try client.pipeline.syncOperations.addHandler(
                    ConnectionIDEventForwarder(
                        onAssociated: { connectionID, context in
                            guard connectionID == first else { return }
                            // As if the peer retired all but the new one, then that one too: it
                            // is the last, so it keeps routing.
                            for connectionID in initial + [first] {
                                context.channel.triggerUserOutboundEvent(
                                    _QUICForTestingRemoveActiveSCIDEvent(scid: connectionID),
                                    promise: nil
                                )
                            }
                            context.channel.triggerUserOutboundEvent(
                                QUICRequestAssociateSCIDEvent(scid: second),
                                promise: nil
                            )
                        },
                        onRetired: { _, _ in }
                    )
                )
            }.get()

            try await client.triggerUserOutboundEvent(QUICRequestAssociateSCIDEvent(scid: first)).get()
            let events = await clientEvents.events { $0 == .retired(first) }
            // The retirement waited for the second one, and a third replaces the first.
            #expect(events == [.associated(first), .associated(second), .retired(first)])
            let replacement = await clientEvents.events { $0.isAssociated }
            let afterwards = try await Self.localConnectionIDs(of: client, as: RecordConsumer.self)
            #expect(afterwards.count == 2)
            #expect(afterwards.first == second)
            let newest = try #require(afterwards.last)
            #expect(replacement == [.associated(newest)])
            #expect(try await Self.retiredConnectionIDs(of: client, as: RecordConsumer.self) == initial + [first])

            try await Self.roundTrip(pair, recording: recording)
        }
    }
}
#endif
