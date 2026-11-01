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

#if DEBUG  // These tests rely on debug only API.

import NIOCore
import NIOEmbedded
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

/// The stream channels of a connection, on stream operations without a connection behind them.
@available(anyAppleOS 26, *)
private final class Harness: QUICStreamChannelGroupDelegate {
    let eventLoop: EmbeddedEventLoop
    let parent: EmbeddedChannel
    let group: QUICStreamChannelGroup
    let table: QUICStreamTable<QUICStreamChannels>
    let operations: TestStreamOperations<QUICStreamChannels>
    var consumer: QUICStreamChannels

    /// The inbound stream initializer.
    var initializer: QUICInboundStreamInitializer?
    /// The inbound streams whose initializer ran, in order.
    private(set) var initialized: [QUICStreamChannel] = []

    init(role: Role = .server, autoRead: Bool = false, bidirectionalLimit: Int = 100) throws {
        self.eventLoop = EmbeddedEventLoop()
        self.parent = EmbeddedChannel(loop: self.eventLoop)
        self.group = QUICStreamChannelGroup(
            parent: self.parent,
            role: role,
            localAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            remoteAddress: SendableAddress(try SocketAddress(ipAddress: "127.0.0.1", port: 4321))
        )
        (self.table, self.operations) = TestStreamOperations<QUICStreamChannels>.makeTable(
            role: role,
            bidirectionalLimit: bidirectionalLimit
        )
        self.group.table = self.table
        self.group.autoRead = autoRead
        self.consumer = QUICStreamChannels(group: self.group)
        self.group.delegate = self
    }

    func initializeInboundStream(_ channel: QUICStreamChannel) {
        self.initialized.append(channel)
        _ = channel.initializeInbound(initializer: self.initializer)
    }

    /// Drains the table, then fires what the drain recorded, as the connection channel does.
    func drain() {
        self.table.drain(into: &self.consumer)
        self.group.deliver()
    }

    /// The connection closed with `error`, as the connection channel closes it.
    func closeConnection(error: (any Error)?) {
        self.table.closeAll(error: error, disconnect: nil, into: &self.consumer)
        self.group.deliver()
        self.group.tableClosed()
    }

    /// The channel of the stream `id`, while it's in the table.
    func channel(_ id: QUICStreamID) -> QUICStreamChannel? {
        guard let handle = self.table.handle(forID: id) else { return nil }
        return self.table.withStream(handle: handle) { _, state in state.channel }
    }

    /// A stream the peer opened, with its channel's `autoRead` set.
    func inboundStream(_ id: QUICStreamID = 0, autoRead: Bool = true) throws -> QUICStreamChannel {
        self.operations.receive("", on: id)
        self.drain()
        let channel = try #require(self.channel(id))
        try channel.syncOptions!.setOption(.autoRead, value: autoRead)
        return channel
    }

    /// Opens a stream, whose channel the returned future gets once `initializer` succeeded.
    func openStream(
        _ type: QUICStreamType = .serverInitiatedBidirectional,
        initializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void> = { channel, _ in
            channel.eventLoop.makeSucceededVoidFuture()
        }
    ) -> EventLoopFuture<any Channel> {
        let promise = self.eventLoop.makePromise(of: (any Channel).self)
        self.group.createOutboundStream(type: type, promise: promise, initializer: initializer)
        return promise.futureResult
    }

    func finish() throws {
        if !self.table.isClosed {
            self.closeConnection(error: nil)
        }
        self.eventLoop.run()
        try self.parent.close().wait()
    }
}

@Suite
struct QUICStreamChannelTests {
    // MARK: - Options

    @available(anyAppleOS 26, *)
    @Test("autoRead starts as the group's and can be configured", arguments: [true, false])
    func autoReadStartsAsTheGroupsAndIsConfigurable(parentAutoRead: Bool) throws {
        let harness = try Harness(autoRead: parentAutoRead)
        defer { try? harness.finish() }
        harness.operations.receive("", on: 0)
        harness.drain()
        let channel = try #require(harness.channel(0))
        let options = try #require(channel.syncOptions)

        #expect(try options.getOption(.autoRead) == parentAutoRead)
        try options.setOption(.autoRead, value: !parentAutoRead)
        #expect(try options.getOption(.autoRead) == !parentAutoRead)
        #expect(try channel.getOption(.autoRead).wait() == !parentAutoRead)
    }

    @available(anyAppleOS 26, *)
    @Test
    func streamIDAndHalfCloseOptions() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(4)
        let options = try #require(channel.syncOptions)

        #expect(try options.getOption(.quicStreamID) == 4)
        #expect(try options.getOption(.halfCloseOnStopSending) == false)
        try options.setOption(.halfCloseOnStopSending, value: true)
        #expect(try options.getOption(.halfCloseOnStopSending) == true)
        #expect(throws: ChannelError.operationUnsupported) {
            try options.getOption(.maxMessagesPerRead)
        }
    }

    // MARK: - Reads

    @available(anyAppleOS 26, *)
    @Test("Calling read twice is idempotent and does not double-deliver")
    func callingReadTwiceIsIdempotent() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let readHolder = ManualReadHandler()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([readHolder, recorder])

        channel.pipeline.read()
        readHolder.releasePendingReadRequest()
        channel.pipeline.read()
        readHolder.releasePendingReadRequest()
        #expect(recorder.events == [.read, .read])

        harness.operations.receive("test", on: 0)
        harness.drain()

        #expect(recorder.events == [.read, .read, .channelRead("test"), .channelReadComplete])
    }

    @available(anyAppleOS 26, *)
    @Test("read called before inbound data arrives", arguments: [true, false])
    func readCalledBeforeInboundDataArrives(autoRead: Bool) throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: autoRead)
        let readHolder = ManualReadHandler()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([readHolder, recorder])

        channel.pipeline.read()
        readHolder.releasePendingReadRequest()
        #expect(recorder.events == [.read])

        harness.operations.receive("test", on: 0)
        harness.drain()

        switch autoRead {
        case false:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete])
            #expect(readHolder.pendingReadRequests.count == 0)
            channel.pipeline.read()
            fallthrough
        case true:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete, .read])
            #expect(readHolder.pendingReadRequests.count == 1)
            // Nothing more to read: the read stays pending, and nothing is fired.
            readHolder.releasePendingReadRequest()
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete, .read])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("read called after inbound data arrives", arguments: [true, false])
    func readCalledAfterInboundDataArrives(autoRead: Bool) throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: autoRead)
        let readHolder = ManualReadHandler()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([readHolder, recorder])

        harness.operations.receive("test", on: 0)
        harness.drain()

        // No read was requested: the bytes stay in the connection.
        #expect(recorder.events == [])
        #expect(harness.operations[0]?.inbound.count == 4)

        channel.pipeline.read()
        readHolder.releasePendingReadRequest()
        #expect(harness.operations[0]?.inbound.count == 0)

        switch autoRead {
        case false:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete])
            channel.pipeline.read()
            fallthrough
        case true:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete, .read])
            #expect(readHolder.pendingReadRequests.count == 1)
            readHolder.releasePendingReadRequest()
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete, .read])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Interleaved read requests and inbound data arrival", arguments: [true, false])
    func interleavedReadRequestsAndInboundDataArrival(autoRead: Bool) throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: autoRead)
        let readHolder = ManualReadHandler()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([readHolder, recorder])

        harness.operations.receive("test", on: 0)
        harness.drain()
        #expect(recorder.events == [])

        channel.pipeline.read()
        readHolder.releasePendingReadRequest()

        for i in 1...3 {
            harness.operations.receive("test\(i)", on: 0)
            harness.drain()
        }

        switch autoRead {
        case false:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete])
            channel.pipeline.read()
            fallthrough
        case true:
            #expect(recorder.events == [.read, .channelRead("test"), .channelReadComplete, .read])
            readHolder.releasePendingReadRequest()
        }

        let expected: [RecordingHandler.Event] = [
            .read, .channelRead("test"), .channelReadComplete,
            .read, .channelRead("test1test2test3"), .channelReadComplete,
        ]
        switch autoRead {
        case false:
            #expect(recorder.events == expected)
        case true:
            #expect(recorder.events == expected + [.read])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A FIN without data fires inputClosed and channelReadComplete on the next read")
    func finOnlyFiresChannelReadComplete() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        harness.operations.receive("", fin: true, on: 0)
        harness.drain()
        #expect(recorder.events == [])

        channel.pipeline.read()
        #expect(recorder.events == [.read, .inputClosed, .channelReadComplete])
    }

    @available(anyAppleOS 26, *)
    @Test("Data with a FIN is delivered to a pending read, then inputClosed")
    func finPacketDeliveredToPendingReadRequest() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let readHolder = ManualReadHandler()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([readHolder, recorder])

        harness.operations.receive("test", fin: true, on: 0)
        harness.drain()
        #expect(recorder.events == [])

        channel.pipeline.read()
        readHolder.releasePendingReadRequest()
        #expect(recorder.events == [.read, .channelRead("test"), .inputClosed, .channelReadComplete])
    }

    /// Several arrivals for one stream in one drain make one read burst.
    @available(anyAppleOS 26, *)
    @Test
    func severalArrivalsInOneDrainMakeOneReadBurst() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)
        channel.pipeline.read()

        harness.operations.receive("first", on: 0)
        harness.operations.receive("second", on: 0)
        harness.drain()

        #expect(recorder.events == [.read, .channelRead("firstsecond"), .channelReadComplete])
    }

    /// Bytes that arrive while the initializer runs wait for it: the reads start once it's done.
    @available(anyAppleOS 26, *)
    @Test
    func bytesArrivingDuringTheInitializerAreReadOnceItIsDone() throws {
        let harness = try Harness(autoRead: true)
        defer { try? harness.finish() }
        let initialized = harness.eventLoop.makePromise(of: Void.self)
        let recorder = RecordingHandler()
        let boundRecorder = NIOLoopBound(recorder, eventLoop: harness.eventLoop)
        harness.initializer = .closure { channel in
            try! channel.pipeline.syncOperations.addHandler(boundRecorder.value)
            return initialized.futureResult
        }

        harness.operations.receive("early", fin: true, on: 0)
        harness.drain()
        #expect(harness.initialized.count == 1)
        #expect(recorder.events == [])

        initialized.succeed()
        #expect(recorder.events == [.read, .channelRead("early"), .inputClosed, .channelReadComplete, .read])
    }

    @available(anyAppleOS 26, *)
    @Test
    func aFailingInboundInitializerClosesTheStream() throws {
        struct Failed: Error {}
        let harness = try Harness()
        defer { try? harness.finish() }
        harness.initializer = .closure { channel in channel.eventLoop.makeFailedFuture(Failed()) }

        harness.operations.receive("", on: 0)
        harness.drain()

        let channel = try #require(harness.initialized.first)
        #expect(!channel.isActive)
        #expect(harness.table.count == 0)
        harness.eventLoop.run()
        try channel.closeFuture.wait()
    }

    // MARK: - Writes

    @available(anyAppleOS 26, *)
    @Test
    func writesAreSentAtTheFlush() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()

        let written = channel.write(ByteBuffer(string: "hello "))
        channel.write(ByteBuffer(string: "world"), promise: nil)
        #expect(harness.operations[0]?.outbound.isEmpty == true)
        // A write succeeds once it's queued, and the channel stays writable.
        try written.wait()
        #expect(channel.isWritable)

        channel.flush()
        #expect(harness.operations[0]?.outboundString == "hello world")
    }

    /// A flush with only empty buffers is a no-op.
    @available(anyAppleOS 26, *)
    @Test
    func flushingAnEmptyBufferSucceeds() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()
        try channel.writeAndFlush(ByteBuffer()).wait()
        #expect(harness.operations[0]?.outbound.isEmpty == true)
    }

    // MARK: - Local close

    @available(anyAppleOS 26, *)
    @Test
    func closingTheOutputSendsAFin() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()

        channel.write(ByteBuffer(string: "bye"), promise: nil)
        try channel.close(mode: .output).wait()
        #expect(harness.operations[0]?.outboundString == "bye")
        #expect(harness.operations[0]?.finished == true)
        #expect(channel.isActive)

        #expect(throws: ChannelError.outputClosed) {
            try channel.close(mode: .output).wait()
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func closingTheInputStopsThePeerAndFiresInputClosed() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        try channel.close(mode: .input).wait()
        #expect(harness.operations[0]?.stopSendingCode == QUICApplicationErrorCode(0))
        #expect(recorder.events == [.inputClosed])
        #expect(channel.isActive)

        #expect(throws: ChannelError.inputClosed) {
            try channel.close(mode: .input).wait()
        }
    }

    /// Closing both sides is graceful: STOP_SENDING with code 0 and a FIN, never RESET_STREAM.
    @available(anyAppleOS 26, *)
    @Test
    func closingBothSidesStopsThePeerAndFinishes() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        channel.write(ByteBuffer(string: "last"), promise: nil)
        try channel.close().wait()

        let stream = try #require(harness.operations[0])
        #expect(stream.outboundString == "last")
        #expect(stream.finished)
        #expect(stream.stopSendingCode == QUICApplicationErrorCode(0))
        #expect(stream.resetCode == nil)
        #expect(recorder.events == [.inputClosed, .channelInactive, .channelUnregistered])
        // The stream left the table.
        #expect(harness.table.count == 0)
    }

    /// As NIO's channels do, the channel is already inactive when its close promise completes.
    @available(anyAppleOS 26, *)
    @Test
    func closePromiseCompletesWithTheChannelInactive() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()
        #expect(channel.isActive)

        var activeWhenClosed: Bool?
        let promise = channel.eventLoop.makePromise(of: Void.self)
        promise.futureResult.whenComplete { _ in activeWhenClosed = channel.isActive }
        channel.close(promise: promise)

        #expect(activeWhenClosed == false)
        // The close future completes on the next loop tick.
        var closed = false
        channel.closeFuture.whenComplete { _ in closed = true }
        #expect(!closed)
        harness.eventLoop.run()
        #expect(closed)
    }

    /// Like NIO's own channels, a stream channel delivers nothing after `channelInactive`: a handler
    /// that closes the stream from `channelRead` sees no `channelReadComplete` after it.
    @available(anyAppleOS 26, *)
    @Test
    func closingFromChannelReadDeliversNothingAfterInactive() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandlers([CloseOnReadHandler(), recorder])

        channel.pipeline.read()
        harness.operations.receive("test", on: 0)
        harness.drain()

        // Closing shuts the read side down, which reports end of stream first.
        #expect(recorder.events == [.read, .inputClosed, .channelInactive, .channelUnregistered])
    }

    @available(anyAppleOS 26, *)
    @Test
    func stopSendingAndResetStreamEventsSendTheirCodes() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()

        try channel.triggerUserOutboundEvent(QUICStopSendingEvent(code: QUICApplicationErrorCode(7)!)).wait()
        #expect(harness.operations[0]?.stopSendingCode == QUICApplicationErrorCode(7))
        #expect(channel.isActive)

        try channel.triggerUserOutboundEvent(QUICResetStreamEvent(code: QUICApplicationErrorCode(9)!)).wait()
        #expect(harness.operations[0]?.resetCode == QUICApplicationErrorCode(9))

        #expect(throws: ChannelError.operationUnsupported) {
            try channel.triggerUserOutboundEvent("unknown").wait()
        }
    }

    /// STOP_SENDING on a stream that can't send any more closes it.
    @available(anyAppleOS 26, *)
    @Test
    func stopSendingEventClosesAStreamWithNothingLeftToSend() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()

        try channel.close(mode: .output).wait()
        try channel.triggerUserOutboundEvent(QUICStopSendingEvent(code: QUICApplicationErrorCode(3)!)).wait()
        #expect(harness.operations[0]?.stopSendingCode == QUICApplicationErrorCode(3))
        #expect(!channel.isActive)
    }

    // MARK: - The peer

    @available(anyAppleOS 26, *)
    @Test
    func peerResetFiresAStreamResetErrorAfterTheBytesRead() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        channel.pipeline.read()
        harness.operations.receive("abc", on: 0)
        harness.drain()
        harness.operations.peerResets(0, code: QUICApplicationErrorCode(5)!)
        harness.drain()

        #expect(
            recorder.events == [
                .read, .channelRead("abc"), .channelReadComplete, .error(.reset(5)), .channelReadComplete,
            ]
        )
        #expect(channel.isActive)

        // Once both sides are done, the connection forgets the stream, and the channel closes.
        try channel.close(mode: .output).wait()
        harness.operations.streamCloses(0)
        harness.drain()
        #expect(!channel.isActive)
        #expect(recorder.events.suffix(2) == [.channelInactive, .channelUnregistered])
    }

    /// A reset that arrives while the initializer runs is fired once it's done.
    @available(anyAppleOS 26, *)
    @Test
    func peerResetDuringTheInitializerIsFiredOnceItIsDone() throws {
        let harness = try Harness(autoRead: true)
        defer { try? harness.finish() }
        let initialized = harness.eventLoop.makePromise(of: Void.self)
        let recorder = RecordingHandler()
        let boundRecorder = NIOLoopBound(recorder, eventLoop: harness.eventLoop)
        harness.initializer = .closure { channel in
            try! channel.pipeline.syncOperations.addHandler(boundRecorder.value)
            return initialized.futureResult
        }

        harness.operations.receive("", on: 0)
        harness.drain()
        harness.operations.peerResets(0, code: QUICApplicationErrorCode(5)!)
        harness.drain()
        #expect(recorder.events == [])

        initialized.succeed()
        #expect(recorder.events == [.error(.reset(5)), .channelReadComplete, .read])
    }

    @available(anyAppleOS 26, *)
    @Test
    func peerStopSendingClosesTheStreamWithAStopSendingError() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        harness.operations.peerStopsSending(0, code: QUICApplicationErrorCode(6)!)
        harness.drain()

        #expect(recorder.events == [.error(.stopSending(6)), .channelInactive, .channelUnregistered])
        // The connection answered with RESET_STREAM; the stream stops the peer as it leaves.
        #expect(harness.operations[0]?.resetCode == nil)
        #expect(harness.operations[0]?.stopSendingCode == QUICApplicationErrorCode(0))
        #expect(harness.table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func peerStopSendingWithHalfCloseFiresAnEventAndKeepsTheInput() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream(autoRead: false)
        try channel.syncOptions!.setOption(.halfCloseOnStopSending, value: true)
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        harness.operations.peerStopsSending(0, code: QUICApplicationErrorCode(6)!)
        harness.drain()
        #expect(recorder.events == [.stopSendingEvent(6)])
        #expect(channel.isActive)

        channel.pipeline.read()
        harness.operations.receive("more", fin: true, on: 0)
        harness.drain()
        #expect(
            recorder.events == [.stopSendingEvent(6), .read, .channelRead("more"), .inputClosed, .channelReadComplete]
        )
    }

    // MARK: - The connection closing

    @available(anyAppleOS 26, *)
    @Test("The connection closing closes the stream with its error", arguments: [false, true])
    func connectionCloseClosesTheStreamWithItsError(withError: Bool) throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()
        let recorder = RecordingHandler()
        try channel.pipeline.syncOperations.addHandler(recorder)

        let error = withError ? QUICConnectionError(reason: "bye", isApplication: true, code: 10) : nil
        harness.closeConnection(error: error)

        #expect(
            recorder.events
                == (withError ? [.error(.connection(10))] : []) + [.channelInactive, .channelUnregistered]
        )
        #expect(!channel.isActive)
        var closed = false
        channel.closeFuture.whenComplete { _ in closed = true }
        harness.eventLoop.run()
        #expect(closed)
    }

    /// A stream the connection closes before its first visit never becomes active.
    @available(anyAppleOS 26, *)
    @Test
    func aStreamClosedBeforeItsFirstVisitSeesNoEvents() throws {
        let harness = try Harness()
        defer { try? harness.finish() }

        harness.operations.receive("data", on: 0)
        harness.closeConnection(error: nil)

        #expect(harness.initialized.isEmpty)
        #expect(harness.table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func remoteAddressFollowsTheActivePath() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let channel = try harness.inboundStream()
        #expect(channel.remoteAddress == (try SocketAddress(ipAddress: "127.0.0.1", port: 4321)))

        let newAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 9001)
        harness.group.remoteAddress.value = newAddress
        #expect(channel.remoteAddress == newAddress)
        #expect(try channel.remoteAddress0() == newAddress)
    }

    @available(anyAppleOS 26, *)
    @Test
    func quiesceReachesEveryStream() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let first = try harness.inboundStream(0)
        let second = try harness.inboundStream(4)
        let recorders = [RecordingHandler(), RecordingHandler()]
        try first.pipeline.syncOperations.addHandler(recorders[0])
        try second.pipeline.syncOperations.addHandler(recorders[1])

        harness.group.quiesce()
        #expect(recorders.map(\.events) == [[.quiesce], [.quiesce]])
    }

    // MARK: - Outbound streams

    @available(anyAppleOS 26, *)
    @Test
    func anOutboundStreamIsHandedOverOnceItsInitializerSucceeded() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        let initialized = harness.eventLoop.makePromise(of: Void.self)
        var initializerSaw: (active: Bool, id: QUICStreamID)?
        let opened = harness.openStream { channel, id in
            initializerSaw = (channel.isActive, id)
            return initialized.futureResult
        }

        harness.drain()
        #expect(initializerSaw?.active == true)
        #expect(initializerSaw?.id == 1)
        var handedOver: (any Channel)?
        opened.whenSuccess { handedOver = $0 }
        #expect(handedOver == nil)

        initialized.succeed()
        let channel = try #require(handedOver)
        #expect(try channel.getOption(.quicStreamID).wait() == 1)

        try channel.writeAndFlush(ByteBuffer(string: "out")).wait()
        #expect(harness.operations[1]?.outboundString == "out")
    }

    @available(anyAppleOS 26, *)
    @Test
    func outboundStreamsBeyondTheStreamLimitWaitForIt() throws {
        let harness = try Harness(bidirectionalLimit: 1)
        defer { try? harness.finish() }
        let first = harness.openStream()
        let second = harness.openStream()
        harness.drain()

        var opened: [QUICStreamID] = []
        first.whenSuccess { channel in opened.append(QUICStreamID(rawValue: try! channel.syncOptions!.getOption(.quicStreamID))) }
        second.whenSuccess { channel in opened.append(QUICStreamID(rawValue: try! channel.syncOptions!.getOption(.quicStreamID))) }
        #expect(opened == [1])

        harness.operations.raiseLimit(bidirectional: true, by: 1)
        harness.drain()
        #expect(opened == [1, 5])
    }

    @available(anyAppleOS 26, *)
    @Test
    func anOutboundStreamWaitingForTheLimitFailsWhenTheConnectionCloses() throws {
        let harness = try Harness(bidirectionalLimit: 0)
        defer { try? harness.finish() }
        let opened = harness.openStream()
        harness.drain()

        harness.closeConnection(error: QUICConnectionError(reason: "", isApplication: false, code: 1))
        #expect(throws: QUICConnectionError.self) { try opened.wait() }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aFailingOutboundInitializerFailsTheOpenAndClosesTheStream() throws {
        struct Failed: Error {}
        let harness = try Harness()
        defer { try? harness.finish() }
        let opened = harness.openStream { channel, _ in channel.eventLoop.makeFailedFuture(Failed()) }
        harness.drain()

        #expect(throws: Failed.self) { try opened.wait() }
        #expect(harness.table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func openingAStreamOfThePeersTypeFails() throws {
        let harness = try Harness(role: .server)
        defer { try? harness.finish() }
        let opened = harness.openStream(.clientInitiatedBidirectional)
        #expect(throws: QUICError.invalidStreamTypeForRole) { try opened.wait() }
    }

    @available(anyAppleOS 26, *)
    @Test
    func openingAStreamOnAClosedConnectionFails() throws {
        let harness = try Harness()
        defer { try? harness.finish() }
        harness.closeConnection(error: nil)
        let opened = harness.openStream()
        #expect(throws: ChannelError.ioOnClosedChannel) { try opened.wait() }
    }

    // MARK: - Re-entrancy

    /// Handlers may write, open streams and close from the events a delivery fires: a delivery
    /// runs outside the drain.
    @available(anyAppleOS 26, *)
    @Test
    func handlersMayActOnStreamsFromTheEventsOfADelivery() throws {
        let harness = try Harness(autoRead: true)
        defer { try? harness.finish() }
        let opened = NIOLoopBoundBox([EventLoopFuture<any Channel>](), eventLoop: harness.eventLoop)
        let group = NIOLoopBound(harness.group, eventLoop: harness.eventLoop)
        harness.initializer = .closure { channel in
            try! channel.pipeline.syncOperations.addHandler(
                EchoAndOpenHandler {
                    let promise = channel.eventLoop.makePromise(of: (any Channel).self)
                    group.value.createOutboundStream(type: .serverInitiatedUnidirectional, promise: promise) {
                        channel,
                        _ in
                        channel.eventLoop.makeSucceededVoidFuture()
                    }
                    opened.value.append(promise.futureResult)
                }
            )
            return channel.eventLoop.makeSucceededVoidFuture()
        }

        harness.operations.receive("ping", fin: true, on: 0)
        harness.drain()

        let stream = try #require(harness.operations[0])
        #expect(stream.outboundString == "ping")
        #expect(stream.finished)
        #expect(opened.value.count == 1)
        // The new stream is opened at the next drain.
        harness.drain()
        let channel = try opened.value[0].wait()
        #expect(try channel.getOption(.quicStreamID).wait() == 3)
    }
}

// MARK: - Handlers

@available(anyAppleOS 26, *)
extension QUICStreamChannelTests {
    /// Records what the pipeline sees.
    fileprivate final class RecordingHandler: ChannelDuplexHandler {
        typealias OutboundIn = ByteBuffer
        typealias InboundIn = ByteBuffer

        enum ErrorKind: Equatable {
            case reset(UInt64)
            case stopSending(UInt64)
            case connection(UInt64)
            case other
        }

        enum Event: Equatable {
            case read
            case channelRead(String)
            case channelReadComplete
            case inputClosed
            case stopSendingEvent(UInt64)
            case quiesce
            case channelInactive
            case channelUnregistered
            case error(ErrorKind)
        }

        var events: [Event] = []

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            self.events.append(.channelRead(String(buffer: self.unwrapInboundIn(data))))
            context.fireChannelRead(data)
        }

        func channelReadComplete(context: ChannelHandlerContext) {
            self.events.append(.channelReadComplete)
            context.fireChannelReadComplete()
        }

        func channelInactive(context: ChannelHandlerContext) {
            self.events.append(.channelInactive)
            context.fireChannelInactive()
        }

        func channelUnregistered(context: ChannelHandlerContext) {
            self.events.append(.channelUnregistered)
            context.fireChannelUnregistered()
        }

        func errorCaught(context: ChannelHandlerContext, error: any Error) {
            switch error {
            case let error as QUICStreamResetError:
                self.events.append(.error(.reset(error.code.rawValue)))
            case let error as QUICStopSendingError:
                self.events.append(.error(.stopSending(error.code.rawValue)))
            case let error as QUICConnectionError:
                self.events.append(.error(.connection(error.code)))
            default:
                self.events.append(.error(.other))
            }
            context.fireErrorCaught(error)
        }

        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            switch event {
            case let event as ChannelEvent where event == .inputClosed:
                self.events.append(.inputClosed)
            case let event as QUICStopSendingEvent:
                self.events.append(.stopSendingEvent(event.code.rawValue))
            case is ChannelShouldQuiesceEvent:
                self.events.append(.quiesce)
            default:
                Issue.record("Unexpected event: \(event)")
            }
            context.fireUserInboundEventTriggered(event)
        }

        func read(context: ChannelHandlerContext) {
            self.events.append(.read)
            context.read()
        }
    }

    /// Holds `read()` requests until they're released.
    fileprivate final class ManualReadHandler: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer

        var pendingReadRequests: [ChannelHandlerContext] = []

        func read(context: ChannelHandlerContext) {
            self.pendingReadRequests.append(context)
        }

        func releasePendingReadRequest() {
            self.pendingReadRequests.popLast()?.read()
        }
    }

    /// Closes the channel when it reads anything, and swallows the read.
    fileprivate final class CloseOnReadHandler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            context.close(promise: nil)
        }
    }

    /// Echoes what it reads, closes the output at the end of the input, and opens a stream.
    fileprivate final class EchoAndOpenHandler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        let open: () -> Void

        init(open: @escaping () -> Void) {
            self.open = open
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            context.writeAndFlush(data, promise: nil)
            self.open()
        }

        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            if event as? ChannelEvent == .inputClosed {
                context.close(mode: .output, promise: nil)
            }
            context.fireUserInboundEventTriggered(event)
        }
    }
}

#endif  // DEBUG
