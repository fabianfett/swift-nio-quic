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
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

@Suite(.timeLimit(.minutes(1)))
struct StreamTableTests {
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testHTTP09Requests(drivers: Drivers) async throws {
        let requestCount = 8
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        let payloads = (0..<requestCount).map { ByteBuffer(string: "GET /quic-\($0)") }

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(failures: failures)
        ) { pair in
            var handles: [QUICStreamHandle] = []
            for payload in payloads {
                handles.append(try await pair.openClientStream(writing: payload, fin: true))
            }

            let visits = await recording.visits(untilClosedStreams: requestCount)

            for (handle, payload) in zip(handles, payloads) {
                let forStream = visits.filter { $0.handle == handle }

                #expect(forStream.allEvents.contains(.opened))
                #expect(forStream.allEvents.contains(.readable))
                #expect(forStream.allEvents.contains(.closed))
                #expect(forStream.last?.bytesReadSoFar == payload)
                #expect(forStream.last?.closeErrorDescription == nil)
            }
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testWriterResumesWhenThePeerRaisesItsFlowControlLimit(drivers: Drivers) async throws {
        // The SwiftNetwork driver never reports `.writable` to a consumer, so a writer that honours
        // `writableBytes` stalls until the idle timeout.
        try await withKnownIssue("SwiftNetwork-driven consumers never see .writable") {
            try await self.writerResumesWhenThePeerRaisesItsFlowControlLimit(drivers: drivers)
        } when: {
            drivers.server == .swiftNetwork
        }
    }

    @available(anyAppleOS 26, *)
    private func writerResumesWhenThePeerRaisesItsFlowControlLimit(drivers: Drivers) async throws {
        // Ten windows' worth, so the server must wait for credit several times.
        let window = 16 * 1024
        let size = 10 * window
        let resumed = NIOLockedValueBox(0)
        let failures = NIOLockedValueBox<[String]>([])
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            maxIdleTimeout: .seconds(2),
            initialMaxStreamData: window,
            client: RecordConsumer(recording: recording),
            server: WindowedSenderConsumer(size: size, resumedAfterBlocking: resumed, failures: failures)
        ) { pair in
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /big"), fin: true)
            let visits = await recording.visits(untilClosedStreams: 1)
            let response = visits.filter { $0.handle == handle }.last?.bytesReadSoFar ?? ByteBuffer()

            #expect(response.readableBytes == size)
            let expected = (0..<size).map(WindowedSenderConsumer.byte(at:))
            #expect(response.readableBytesView.elementsEqual(expected))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
        #expect(resumed.withLockedValue { $0 } > 0, "the server never had to wait for credit")
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testManyHTTP09RequestStreams(drivers: Drivers) async throws {
        let streamCount = 2000
        let request = ByteBuffer(string: "GET /quic")
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: RequestOnOpenConsumer(request: request, recording: recording, failures: failures),
            server: EchoConsumer(failures: failures)
        ) { pair in
            // The consumer writes when the stream gets its opened notification (many of the open
            // streams will be pending until they get stream credit.)
            for _ in 0..<streamCount {
                _ = try await pair.openClientStream(state: ByteBuffer())
            }

            let visits = await recording.visits(untilClosedStreams: streamCount)

            let closed = visits.filter { $0.events.contains(.closed) }
            #expect(Set(closed.map { $0.handle }).count == streamCount)
            #expect(closed.allSatisfy { $0.bytesReadSoFar == request })
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testHTTP09Streaming(drivers: Drivers) async throws {
        let requestCount = 4
        let chunkSize = 1024 * 1024 / 10
        let chunkCount = 10
        let expected = StreamingConsumer.expectedResponse(chunkSize: chunkSize, chunkCount: chunkCount)

        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: StreamingConsumer(chunkSize: chunkSize, chunkCount: chunkCount, failures: failures)
        ) { pair in
            for _ in 0..<requestCount {
                _ = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: true)
            }

            let visits = await recording.visits(untilClosedStreams: requestCount)

            let responses = visits.filter { $0.events.contains(.closed) }.map { $0.bytesReadSoFar }
            #expect(responses.count == requestCount)
            #expect(responses.allSatisfy { $0 == expected })
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testHTTP09ManyStreamsStreaming(drivers: Drivers) async throws {
        let requestCount = 2000
        let chunkSize = 32
        let chunkCount = 5
        let expected = StreamingConsumer.expectedResponse(chunkSize: chunkSize, chunkCount: chunkCount)

        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            initialMaxStreamsBidi: requestCount,
            client: RecordConsumer(recording: recording),
            server: StreamingConsumer(chunkSize: chunkSize, chunkCount: chunkCount, failures: failures)
        ) { pair in
            for _ in 0..<requestCount {
                _ = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: true)
            }

            let visits = await recording.visits(untilClosedStreams: requestCount)

            let responses = visits.filter { $0.events.contains(.closed) }.map { $0.bytesReadSoFar }
            #expect(responses.count == requestCount)
            #expect(responses.allSatisfy { $0 == expected })
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testConnectionError(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let serverConnection = NIOLockedValueBox<QUICStreamConnection<EchoConsumer>?>(nil)
        let captureConnection: @Sendable (QUICStreamConnection<EchoConsumer>) -> Void = { connection in
            serverConnection.withLockedValue { $0 = connection }
        }

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(),
            onServerConnection: captureConnection
        ) { pair in
            // Finish one request first: the server only echoes after it has processed the client's Finished,
            // so its close can't land in a Handshake packet.
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "ping"), fin: true)
            _ = await recording.visits(untilClosedStreams: 1)

            _ = try await pair.openClientStream(
                writing: ByteBuffer(string: "GET /quic"),
                fin: false
            )

            let connection = try #require(serverConnection.withLockedValue { $0 })
            try await connection.channel.triggerUserOutboundEvent(
                QUICCloseConnectionEvent(code: QUICApplicationErrorCode(10)!, reasonPhrase: "test")
            )

            let visits = await recording.visits { $0.events.contains(.closed) }
            let error = try #require(visits.last?.closeError as? QUICConnectionError)

            #expect(error.code == 10)
            #expect(error.reason == "test")
            #expect(error.isApplication)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testResetStream(drivers: Drivers) async throws {
        let code = QUICApplicationErrorCode(42)!
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasReset in
                    if hasReset { return }
                    hasReset = true
                    stream.sendResetStream(code: code)
                }
            }
        ) { pair in
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

            let visits = await recording.visits { $0.events.contains(.reset) }
            let reset = try #require(visits.last)

            #expect(reset.resetCode == code)
            #expect(!reset.isReceiveOpen)
            #expect(reset.isSendOpen)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testResetStreamRaceCondition(drivers: Drivers) async throws {
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasReset in
                    if hasReset { return }
                    hasReset = true
                    stream.sendResetStream(code: QUICApplicationErrorCode(10)!)
                }
            }
        ) { pair in
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

            try await pair.clientConnection.channel.close()

            let visits = await recording.visits(untilClosedStreams: 1)
            #expect(visits.contains { $0.handle == handle && $0.events.contains(.closed) })

            let remaining = try await pair.clientConnection.withStreams { $0._table.count }.get()
            #expect(remaining == 0)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testStopSending(drivers: Drivers) async throws {
        let code = QUICApplicationErrorCode(10)!
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasStopped in
                    if hasStopped { return }
                    hasStopped = true
                    stream.sendStopSending(code: code)
                }
            }
        ) { pair in
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

            let visits = await recording.visits { $0.events.contains(.stopSending) }
            let stopped = try #require(visits.last)

            #expect(stopped.stopSendingCode == code)
            #expect(!stopped.isSendOpen)
            #expect(stopped.isReceiveOpen)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testClientInitiatedUnidirectionalStreamStopSending(drivers: Drivers) async throws {
        let code = QUICApplicationErrorCode(10)!
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasStopped in
                    if hasStopped { return }
                    hasStopped = true
                    stream.sendStopSending(code: code)
                }
            }
        ) { pair in
            let handle = try await pair.openClientStream(.clientInitiatedUnidirectional, state: ByteBuffer())
            try await pair.write(ByteBuffer(string: "Hello from client"), to: handle, fin: false)

            let visits = await recording.visits { $0.events.contains(.stopSending) }
            let stopped = try #require(visits.last)

            #expect(stopped.handle == handle)
            #expect(stopped.stopSendingCode == code)
            #expect(!stopped.isSendOpen)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testServerInitiatedUnidirectionalStreamStopSending(drivers: Drivers) async throws {
        let code = QUICApplicationErrorCode(10)!
        let serverRecording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasStopped in
                    if hasStopped || stream.isSendOpen { return }
                    hasStopped = true
                    stream.sendStopSending(code: code)
                }
            },
            server: OpeningConsumer(
                type: .serverInitiatedUnidirectional,
                payload: ByteBuffer(string: "Hello from server"),
                fin: false,
                recording: serverRecording,
                failures: failures
            )
        ) { pair in
            // Poke the server so it gets a visit and opens its stream back.
            _ = try await pair.openClientStream(
                state: false,
                writing: ByteBuffer(string: "GET /quic"),
                fin: false
            )

            let visits = await serverRecording.visits { $0.events.contains(.stopSending) }
            let stopped = try #require(visits.last)

            #expect(stopped.stopSendingCode == code)
            #expect(QUICStreamType(try #require(stopped.id)) == .serverInitiatedUnidirectional)
            #expect(!stopped.isSendOpen)
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testStopSendingOnlyHalfClosesInput(drivers: Drivers) async throws {
        let serverRecording = StreamRecording()

        var buffer = ByteBuffer(string: "GET /quic")
        buffer.writeString("Hello after stopSending")
        let expected = buffer

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: StreamRecording()),
            server: RecordConsumer(recording: serverRecording)
        ) { pair in
            let handle = try await pair.openClientStream(
                state: ByteBuffer(),
                writing: ByteBuffer(string: "GET /quic"),
                fin: false
            )

            // Close the receive side, then keep writing on the send side.
            _ = try await pair.clientConnection.withStreams { streams in
                try streams.withStream(handle: handle) { stream, _ in
                    stream.sendStopSending(code: QUICApplicationErrorCode(42)!)
                    stream.write(ByteBuffer(string: "Hello after stopSending"))
                    try stream.flush(fin: true)
                }
            }.get()

            let serverVisits = await serverRecording.visits { $0.bytesReadSoFar == expected }
            #expect(serverVisits.last?.bytesReadSoFar == expected)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testResetStreamOnlyHalfClosesOutput(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasResponded in
                    if hasResponded || !stream.isSendOpen { return }

                    hasResponded = true
                    stream.write(ByteBuffer(string: "<b>Success</b>"))
                    do {
                        try stream.flush(fin: true)
                    } catch {
                        failures.withLockedValue { $0.append("response flush failed: \(error)") }
                    }
                }
            }
        ) { pair in
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

            _ = try await pair.clientConnection.withStreams { streams in
                streams.withStream(handle: handle) { stream, _ in
                    stream.sendResetStream(code: QUICApplicationErrorCode(42)!)
                }
            }.get()

            let visits = await recording.visits {
                $0.bytesReadSoFar == ByteBuffer(string: "<b>Success</b>")
            }
            #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "<b>Success</b>"))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testReceivedStopSendingHalfClosesOutput(drivers: Drivers) async throws {
        let code = QUICApplicationErrorCode(10)!
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: CallbackConsumer(makeState: { false }) { visit in
                var visit = visit
                visit.withStream { stream, hasStopped in
                    if hasStopped { return }
                    hasStopped = true
                    stream.sendStopSending(code: code)
                }
            }
        ) { pair in
            _ = try await pair.openClientStream(
                writing: ByteBuffer(string: "GET /quic"),
                fin: false
            )

            let visits = await recording.visits { $0.events.contains(.stopSending) }
            let stopped = try #require(visits.last)

            #expect(stopped.stopSendingCode == code)
            #expect(stopped.isReceiveOpen)
            #expect(!stopped.events.contains(.closed))
            #expect(stopped.closeError == nil)
        }
    }

    #if DEBUG
    /// A write the connection can no longer send fails its flush, even before the stream learns
    /// that the connection is closing (RFC 9000 Section 10.2: a closing endpoint sends no more
    /// stream data).
    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testFlushFailsOnceTheConnectionCanNoLongerSend(drivers: Drivers) async throws {
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer()
        ) { pair in
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /"), fin: false)

            let outcome = try await pair.clientConnection.withStreams { streams -> (Bool, String?) in
                let channel = pair.clientConnection.channel as! QUICConnectionChannel<RecordConsumer>
                channel._forTesting_closeConnectionOnly()
                var wasSendOpen = false
                do {
                    try streams.withStream(handle: handle) { stream, _ in
                        wasSendOpen = stream.isSendOpen
                        stream.write(ByteBuffer(string: "quic"))
                        try stream.flush(fin: true)
                    }
                    return (wasSendOpen, nil)
                } catch {
                    return (wasSendOpen, "\(error)")
                }
            }.get()
            let (wasSendOpen, flushError) = try #require(outcome)

            // The stream doesn't know yet: the flush gets as far as the connection.
            #expect(wasSendOpen)
            // Both drivers throw the stack's "not connected" error.
            #expect(flushError != nil)
        }
    }
    #endif

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testShutdownForcefully(drivers: Drivers) async throws {
        final class CountedState {
            let counter: Counter

            init(counter: Counter) {
                self.counter = counter
                self.counter.increment()
            }

            deinit {
                self.counter.decrement()
            }
        }

        let counter = Counter()
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: CallbackConsumer(makeState: { CountedState(counter: counter) }) { visit in
                var visit = visit
                recording.record(StreamRecording.Visit(&visit))
            },
            server: EchoConsumer()
        ) { pair in
            var opened: [QUICStreamHandle] = []
            for _ in 0..<3 {
                opened.append(
                    try await pair.openClientStream(
                        state: CountedState(counter: counter),
                        writing: ByteBuffer(string: "GET /quic"),
                        fin: false
                    )
                )
            }

            #expect(counter.load() == 3)

            try await pair.clientConnection.channel.close()

            let visits = await recording.visits(untilClosedStreams: 3)

            let closed = visits.filter { $0.events.contains(.closed) }
            #expect(Set(closed.map { $0.handle }) == Set(opened))

            // Shutting a server down which is already shutting down must not fail.
            let handle = try await pair.serverChannel.pipeline
                .handler(type: QUICHandler<EchoConsumer>.self)
                .map { $0.makeHandle() }
                .get()

            try await handle.shutdownGracefully(deadline: .now())
            try await handle.shutdownGracefully(deadline: .now())
        }

        #expect(counter.load() == 0)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func testCloseInput(drivers: Drivers) async throws {
        /// The server's per-stream state: the request so far, and whether it has responded.
        struct Request {
            var bytes = ByteBuffer()
            var responded = false
        }

        let request = ByteBuffer(string: "GET /quic")
        let response = ByteBuffer(string: "<b>Success</b>")

        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])
        let serverSawFIN = NIOLockedValueBox(false)

        try await withConsumerPair(
            drivers: drivers,
            // Reads the response, then closes its output: the server only finishes after that FIN.
            client: CallbackConsumer(makeState: { ByteBuffer() }) { visit in
                var visit = visit
                visit.withStream { stream, buffer in
                    reading: while true {
                        switch stream.read(into: &buffer) {
                        case .read:
                            ()
                        case .nothingAvailable, .endOfStream:
                            break reading
                        }
                    }

                    if buffer == response && stream.isSendOpen {
                        do {
                            try stream.flush(fin: true)
                        } catch {
                            failures.withLockedValue { $0.append("client FIN failed: \(error)") }
                        }
                    }
                }

                recording.record(StreamRecording.Visit(reading: &visit))
            },
            server: CallbackConsumer(makeState: { Request() }) { visit in
                var visit = visit
                visit.withStream { stream, state in
                    var atEndOfStream = false
                    reading: while true {
                        switch stream.read(into: &state.bytes) {
                        case .read:
                            ()
                        case .nothingAvailable:
                            break reading
                        case .endOfStream:
                            atEndOfStream = true
                            break reading
                        }
                    }

                    do {
                        if !state.responded && state.bytes == request {
                            state.responded = true
                            stream.write(response)
                            try stream.flush(fin: false)
                        }

                        // Only finish our output once the client has closed its input.
                        if atEndOfStream && stream.isSendOpen {
                            serverSawFIN.withLockedValue { $0 = true }
                            try stream.flush(fin: true)
                        }
                    } catch {
                        failures.withLockedValue { $0.append("server flush failed: \(error)") }
                    }
                }
            }
        ) { pair in
            let handle = try await pair.openClientStream(state: ByteBuffer(), writing: request, fin: false)

            let visits = await recording.visits(untilClosedStreams: 1)
            let last = try #require(visits.last { $0.handle == handle })

            #expect(last.bytesReadSoFar == response)
            #expect(last.events.contains(.closed))
            #expect(last.closeError == nil)
            #expect(serverSawFIN.withLockedValue { $0 })
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    // MARK: - Table specific tests

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func manyConcurrentStreamsKeepTheirOwnState(drivers: Drivers) async throws {
        let streamCount = 2000
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        let payloads = (0..<streamCount).map { ByteBuffer(string: "request-\($0)") }

        try await withConsumerPair(
            drivers: drivers,
            initialMaxStreamsBidi: streamCount,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(failures: failures)
        ) { pair in
            for payload in payloads {
                _ = try await pair.openClientStream(state: ByteBuffer(), writing: payload, fin: true)
            }

            let visits = await recording.visits(untilClosedStreams: streamCount)

            let echoed = Set(visits.filter { $0.events.contains(.closed) }.map { $0.bytesReadSoFar })
            #expect(echoed == Set(payloads))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func inboundStreamReachesTheConsumer(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: OpeningConsumer(
                type: .serverInitiatedBidirectional,
                payload: ByteBuffer(string: "Server initiated"),
                recording: StreamRecording(),
                failures: failures
            )
        ) { pair in
            // Poke the server so it gets a visit and opens its stream back.
            _ = try await pair.openClientStream(
                state: ByteBuffer(),
                writing: ByteBuffer(string: "GET /quic"),
                fin: true
            )

            let visits = await recording.visits {
                $0.bytesReadSoFar == ByteBuffer(string: "Server initiated")
            }
            let inbound = try #require(visits.last)

            #expect(inbound.bytesReadSoFar == ByteBuffer(string: "Server initiated"))
            #expect(QUICStreamType(try #require(inbound.id)) == .serverInitiatedBidirectional)

            let inboundVisits = visits.filter { $0.handle == inbound.handle }
            #expect(inboundVisits.allEvents.contains(.opened))
            #expect(inboundVisits.allEvents.contains(.readable))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func unidirectionalStreamIsSendOnlyAtTheOpener(drivers: Drivers) async throws {
        let clientRecording = StreamRecording()
        let serverRecording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: clientRecording),
            server: RecordConsumer(recording: serverRecording)
        ) { pair in
            let handle = try await pair.openClientStream(
                .clientInitiatedUnidirectional,
                state: ByteBuffer()
            )

            let clientVisits = await clientRecording.visits { $0.events.contains(.opened) }
            let opener = try #require(clientVisits.last)
            #expect(opener.isSendOpen)
            #expect(!opener.isReceiveOpen)

            try await pair.write(ByteBuffer(string: "send only"), to: handle, fin: false)

            let serverVisits = await serverRecording.visits {
                $0.bytesReadSoFar == ByteBuffer(string: "send only")
            }
            let peer = try #require(serverVisits.last)
            #expect(!peer.isSendOpen)
            #expect(peer.isReceiveOpen)
            #expect(peer.bytesReadSoFar == ByteBuffer(string: "send only"))
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func clientCannotOpenAServerInitiatedStream(drivers: Drivers) async throws {
        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: StreamRecording()),
            server: EchoConsumer()
        ) { pair in
            let outcome = try await pair.clientConnection.withStreams { streams -> (thrown: (any Error)?, count: Int) in
                do {
                    _ = try streams.open(.serverInitiatedBidirectional, state: ByteBuffer())
                    return (nil, streams._table.count)
                } catch {
                    return (error, streams._table.count)
                }
            }.get()

            #expect(outcome?.thrown as? QUICError == QUICError.invalidStreamTypeForRole)
            #expect(outcome?.count == 0)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func largePayloadArrives(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(failures: failures)
        ) { pair in
            _ = try await pair.openClientStream(
                state: ByteBuffer(),
                writing: ByteBuffer(repeating: 0xAB, count: 1 << 20),
                fin: true
            )

            let visits = await recording.visits { $0.events.contains(.closed) }
            #expect(visits.last?.bytesReadSoFar == ByteBuffer(repeating: 0xAB, count: 1 << 20))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func closedConnectionCannotOpenStreams(drivers: Drivers) async throws {
        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: StreamRecording()),
            server: EchoConsumer()
        ) { pair in
            try await pair.clientConnection.channel.close()

            // The table outlives the connection, so 'withStreams' still resolves.
            let openFailed = try await pair.clientConnection.withStreams { streams -> Bool in
                do {
                    _ = try streams.open(.clientInitiatedBidirectional, state: ByteBuffer())
                    return false
                } catch {
                    return true
                }
            }.get()

            #expect(openFailed == true)
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func forceCloseClosesEveryStream(drivers: Drivers) async throws {
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer()
        ) { pair in
            // No FIN, so the stream is still open when the connection is forced closed.
            let handle = try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)

            // What the registry does once a graceful shutdown overruns its deadline.
            let channel = try #require(pair.clientConnection.channel as? QUICConnectionChannel<RecordConsumer>)
            try await channel.eventLoop.submit {
                channel.transportView.forceClose()
            }.get()
            // Completes once the channel has torn down, which retires the table's anchor.
            try await channel.closeFuture.get()

            // The stream closed with the connection: its handle no longer resolves, so there is
            // nothing left to flush.
            let resolved = try await pair.clientConnection.withStreams { streams -> Bool in
                try streams.withStream(handle: handle) { stream, _ in
                    try stream.flush(fin: true)
                } != nil
            }.get()

            #expect(resolved == false)

            let visits = await recording.visits(untilClosedStreams: 1)
            let closed = visits.filter { $0.events.contains(.closed) }
            #expect(closed.map { $0.handle } == [handle])
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func creditBlockedStreamIsOpenedOnlyOnceItHasAnID(drivers: Drivers) async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            drivers: drivers,
            initialMaxStreamsBidi: 2,
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(failures: failures)
        ) { pair in
            var handles: [QUICStreamHandle] = []
            for _ in 0..<3 {
                handles.append(try await pair.openClientStream(state: ByteBuffer()))
            }

            // The third stream is over the peer's limit, so the stack has no ID for it yet and a
            // flush is refused rather than trapping.
            let blocked = handles[2]
            let flushedWhileBlocked = try await pair.clientConnection.withStreams { streams -> Bool in
                do {
                    try streams.withStream(handle: blocked) { stream, _ in
                        stream.write(ByteBuffer(string: "blocked"))
                        try stream.flush(fin: true)
                    }
                    return true
                } catch {
                    return false
                }
            }.get()

            #expect(flushedWhileBlocked == false)

            // Drive the first two streams to completion, which frees the credit for the third.
            for handle in handles[0..<2] {
                try await pair.write(ByteBuffer(string: "request"), to: handle, fin: true)
            }

            let visits = await recording.visits {
                $0.handle == blocked && $0.events.contains(.opened)
            }
            let blockedVisits = visits.filter { $0.handle == blocked }

            // A stream isn't presented at all until it has an ID, and 'opened' is how it arrives.
            #expect(blockedVisits.count == 1)
            #expect(blockedVisits.first?.events == .opened)
            #expect(blockedVisits.first?.id != nil)

            // The refused flush can be repeated now the stream has an ID.
            let flushedWhenOpened = try await pair.clientConnection.withStreams { streams -> Bool in
                do {
                    try streams.withStream(handle: blocked) { stream, _ in
                        try stream.flush(fin: true)
                    }
                    return true
                } catch {
                    return false
                }
            }.get()

            #expect(flushedWhenOpened == true)
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func idleTimeoutClosesEveryStream(drivers: Drivers) async throws {
        let streamCount = 2
        let recording = StreamRecording()

        try await withConsumerPair(
            drivers: drivers,
            maxIdleTimeout: .seconds(1),
            client: RecordConsumer(recording: recording),
            server: EchoConsumer()
        ) { pair in
            // Nothing is written after this, so both ends go idle and the timeout fires.
            var handles: [QUICStreamHandle] = []
            for _ in 0..<streamCount {
                handles.append(
                    try await pair.openClientStream(writing: ByteBuffer(string: "GET /quic"), fin: false)
                )
            }

            let visits = await recording.visits(untilClosedStreams: streamCount)
            let closed = visits.filter { $0.events.contains(.closed) }

            #expect(Set(closed.map { $0.handle }) == Set(handles))
            #expect(closed.allSatisfy { $0.closeError == nil })
        }
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: Drivers.all)
    func localConnectionCloseReachesThePeersConsumer(drivers: Drivers) async throws {
        /// Records every visit and reports the one which reads the request.
        struct RequestSignallingConsumer: QUICStreamConsumer {
            typealias StreamState = ByteBuffer

            let request: ByteBuffer
            let recording: StreamRecording
            let read: EventLoopPromise<Void>

            func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
                ByteBuffer()
            }

            mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
                while var visit = streams.next() {
                    let recorded = StreamRecording.Visit(reading: &visit)
                    self.recording.record(recorded)

                    if recorded.bytesReadSoFar == self.request {
                        self.read.succeed()
                    }
                }
            }
        }

        let code = QUICApplicationErrorCode(20)!
        let request = ByteBuffer(string: "GET /quic")
        let serverRecording = StreamRecording()
        let serverReadRequest = makePromise(of: Void.self, timeout: .seconds(10))

        try await withConsumerPair(
            drivers: drivers,
            client: RecordConsumer(recording: StreamRecording()),
            server: RequestSignallingConsumer(
                request: request,
                recording: serverRecording,
                read: serverReadRequest
            )
        ) { pair in
            _ = try await pair.openClientStream(writing: request, fin: false)

            // Wait for the server to have the stream, so the close has something to land on.
            try await serverReadRequest.futureResult.get()

            try await pair.clientConnection.channel.triggerUserOutboundEvent(
                QUICCloseConnectionEvent(code: code, reasonPhrase: "client says bye")
            )

            let visits = await serverRecording.visits(untilClosedStreams: 1)
            let closed = try #require(visits.last { $0.events.contains(.closed) })
            let error = try #require(closed.closeError as? QUICConnectionError)

            #expect(error.code == 20)
            #expect(error.reason == "client says bye")
            #expect(error.isApplication)
        }
    }
}
