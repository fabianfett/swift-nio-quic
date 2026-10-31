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

/// The sizes of the datagrams a connection received.
struct ReceivedSizes: Sendable {
    var count = 0
    var largest = 0
    /// The datagrams larger than 1200 bytes, the size both sides start with.
    var larger = 0

    mutating func record(_ datagram: ByteBuffer) {
        self.count += 1
        self.largest = max(self.largest, datagram.readableBytes)
        if datagram.readableBytes > 1200 {
            self.larger += 1
        }
    }
}

/// A state machine connection configured with `InitialPacketSize.matchingClientInitial(upTo:)`
/// searches for larger packets up to the maximum with PMTUD (RFC 9000 Section 14.3), once the
/// handshake is confirmed and enough stream data waits to pay for the probes; one with a fixed
/// size never sends larger packets.
@Suite(.timeLimit(.minutes(1)))
struct StateMachinePMTUDTests {
    /// The bytes of each request or response: four of them make the probes up to 1400 bytes worth
    /// their cost to the core.
    private static let transferSize = 2 * 1024 * 1024
    private static let streams = 4
    /// The rounds of transfers a search may take.
    private static let maximumRounds = 4

    /// Whether the platform sends runs of datagrams with UDP GSO: NIO supports it on Linux.
    private static var segments: Bool {
        #if os(Linux) || os(Android)
        true
        #else
        false
        #endif
    }

    @available(anyAppleOS 26, *)
    private static func connection<Consumer: QUICStreamConsumer>(
        _ channel: any Channel,
        as consumer: Consumer.Type
    ) -> StateMachineQUICConnection<Consumer> {
        (channel as! QUICConnectionChannel<Consumer>)._forTesting_stateMachineConnection!
    }

    /// The server matches the client's Initial of 1200 bytes and probes up to the maximum: 1400
    /// bytes over IPv4 and IPv6. A maximum of 1250 leaves nothing to probe for over IPv4, where
    /// the search starts at an MTU of 1280 bytes, 1252 bytes of payload, but over IPv6 it reaches
    /// 1250 bytes. A fixed size never grows.
    ///
    /// On Linux the server sends runs of datagrams with UDP GSO, and the probes, larger than the
    /// runs' segment size, in envelopes of their own; elsewhere a handler splits runs as the
    /// kernel does.
    @available(anyAppleOS 26, *)
    @Test(arguments: [
        (InitialPacketSize.matchingClientInitial(upTo: 1400), "127.0.0.1", 1400),
        (InitialPacketSize.matchingClientInitial(upTo: 1400), "::1", 1400),
        (InitialPacketSize.matchingClientInitial(upTo: 1250), "127.0.0.1", 1200),
        (InitialPacketSize.matchingClientInitial(upTo: 1250), "::1", 1250),
        (InitialPacketSize.fixed(1200), "127.0.0.1", 1200),
    ])
    func aServerProbesUpToTheMaximum(size: InitialPacketSize, host: String, expected: Int) async throws {
        let received = NIOLockedValueBox(ReceivedSizes())
        let recording = NIOLockedValueBox(DatagramRecording())
        let (responses, continuation) = AsyncStream.makeStream(of: PatternChecker.Response.self)
        var iterator = responses.makeAsyncIterator()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<PatternResponder>?>(nil)

        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            host: host,
            configureServer: { $0.initialPacketSize = size },
            inspectClientDatagram: { datagram in received.withLockedValue { $0.record(datagram) } },
            configureClientPipeline: { try $0.addHandler(LargeReceiveBuffer()) },
            configureServerPipeline: { pipeline in
                try pipeline.addHandler(DatagramRecorder(recording: recording, splitting: !Self.segments))
            },
            client: PatternChecker(responses: continuation),
            server: PatternResponder(responseSize: Self.transferSize),
            onServerConnection: { connection in serverConnection.withLockedValue { $0 = connection } },
            { pair in
                let server = try #require(serverConnection.withLockedValue { $0 })
                // Runs of datagrams on every platform.
                try await server.channel.eventLoop.submit {
                    Self.connection(server.channel, as: PatternResponder.self)._forTesting_maximumSegments = 64
                }.get()
                var datagramSize = 0
                var rounds = 0
                while datagramSize < expected, rounds < Self.maximumRounds {
                    rounds += 1
                    for _ in 0..<Self.streams {
                        _ = try await pair.openClientStream(
                            state: PatternChecker.Response(),
                            writing: ByteBuffer(string: "GET /"),
                            fin: true
                        )
                    }
                    for _ in 0..<Self.streams {
                        let response = await iterator.next()
                        #expect(response == PatternChecker.Response(length: Self.transferSize))
                    }
                    datagramSize = try await server.channel.eventLoop.submit {
                        Self.connection(server.channel, as: PatternResponder.self)._forTesting_datagramSize
                    }.get()
                }

                #expect(datagramSize == expected, "after \(rounds) rounds")
                let sizes = received.withLockedValue { $0 }
                #expect(sizes.largest == expected, "\(sizes)")
                if expected > 1200 {
                    #expect(sizes.larger > 0)
                } else {
                    #expect(sizes.larger == 0)
                }
                let sent = recording.withLockedValue { $0 }
                #expect(sent.datagrams.allSatisfy { $0.readableBytes <= expected })
                #expect(sent.malformedEnvelopes == 0)
                #expect(sent.failedWrites == [])
                #expect(sent.segmentedEnvelopes > 0)
            }
        )
    }

    /// A client probes up to its maximum as a server does, here while it uploads; a client with a
    /// fixed size doesn't. Clients start with 1200 bytes.
    @available(anyAppleOS 26, *)
    @Test(arguments: [
        (InitialPacketSize.matchingClientInitial(upTo: 1400), 1400), (InitialPacketSize.fixed(1200), 1200),
    ])
    func aClientProbesUpToTheMaximum(size: InitialPacketSize, expected: Int) async throws {
        let received = NIOLockedValueBox(ReceivedSizes())
        let (uploads, continuation) = AsyncStream.makeStream(of: PatternChecker.Response.self)
        var iterator = uploads.makeAsyncIterator()
        // The server doesn't answer.
        let (_, responses) = AsyncStream.makeStream(of: Int.self)
        let upload = ByteBuffer(bytes: (0..<Self.transferSize).map { UInt8(truncatingIfNeeded: $0 % 251) })

        try await withConsumerPair(
            drivers: Drivers(client: .stateMachine, server: .stateMachine),
            // The server doesn't close the streams.
            initialMaxStreamsBidi: Self.streams * Self.maximumRounds,
            configureClient: { $0.initialPacketSize = size },
            configureServerPipeline: { pipeline in
                try pipeline.addHandler(LargeReceiveBuffer())
                try pipeline.addHandler(
                    DatagramInspector(inspect: { datagram in received.withLockedValue { $0.record(datagram) } })
                )
            },
            client: ByteCountingConsumer(finished: responses),
            server: PatternChecker(responses: continuation),
            { pair in
                var datagramSize = 0
                var rounds = 0
                while datagramSize < expected, rounds < Self.maximumRounds {
                    rounds += 1
                    for _ in 0..<Self.streams {
                        _ = try await pair.openClientStream(state: 0, writing: upload, fin: true)
                    }
                    for _ in 0..<Self.streams {
                        let upload = await iterator.next()
                        #expect(upload == PatternChecker.Response(length: Self.transferSize))
                    }
                    let client = pair.clientConnection.channel
                    datagramSize = try await client.eventLoop.submit {
                        Self.connection(client, as: ByteCountingConsumer.self)._forTesting_datagramSize
                    }.get()
                }

                #expect(datagramSize == expected, "after \(rounds) rounds")
                let sizes = received.withLockedValue { $0 }
                #expect(sizes.largest == expected, "\(sizes)")
                if expected > 1200 {
                    #expect(sizes.larger > 0)
                } else {
                    #expect(sizes.larger == 0)
                }
            }
        )
    }
}
#endif
