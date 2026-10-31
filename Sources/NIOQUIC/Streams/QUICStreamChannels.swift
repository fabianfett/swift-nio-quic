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

import NIOCore
import NIOQUICHelpers

/// A consumer which provides a `Channel` per QUIC stream.
///
/// The handler makes it for connections whose streams are surfaced as child channels, see
/// ``QUICHandler``: it can't be made otherwise.
@available(anyAppleOS 26, *)
public struct QUICStreamChannels: QUICStreamConsumer {
    /// The channel of a stream.
    public struct StreamState {
        let channel: QUICStreamChannel

        init(channel: QUICStreamChannel) {
            self.channel = channel
        }
    }

    /// What the stream channels of the connection share.
    let group: QUICStreamChannelGroup

    init(group: QUICStreamChannelGroup) {
        self.group = group
    }

    public mutating func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState {
        StreamState(channel: QUICStreamChannel(inbound: stream.handle, group: self.group))
    }

    /// Records what happened to each stream on its channel, and reads for those with a read
    /// pending. Runs no pipeline code: the connection channel calls the group's
    /// ``QUICStreamChannelGroup/deliver()`` once the drain has finished, which fires the events.
    public mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while var visit = streams.next() {
            let channel = visit.state.channel
            channel.record(
                events: visit.events,
                id: visit.id,
                resetCode: visit.resetCode,
                stopSendingCode: visit.stopSendingCode,
                closeError: visit.closeError
            )
            if channel.wantsToRead(visit.events) {
                visit.withStream { stream, _ in
                    channel.read(from: &stream)
                }
            }
        }
    }
}

/// Runs the initializer of an inbound stream channel, for its connection.
@available(anyAppleOS 26, *)
protocol QUICStreamChannelGroupDelegate: AnyObject {
    /// `channel` became active: run the connection's inbound stream initializer on it, see
    /// ``QUICStreamChannel/initializeInbound(initializer:)``.
    func initializeInboundStream(_ channel: QUICStreamChannel)
}

/// What the stream channels of a connection share: the stream table they run on, the connection
/// channel, and the channels with events to deliver once a drain of the table has finished.
@available(anyAppleOS 26, *)
final class QUICStreamChannelGroup {
    let eventLoop: any EventLoop
    let allocator: ByteBufferAllocator
    let role: Role
    let localAddress: SocketAddress?
    /// The address of the remote peer, which follows the connection's active path.
    let remoteAddress: SendableAddress

    /// The connection channel, the parent of the stream channels made from now on. Held strongly
    /// until the table closes, see ``tableClosed()``: the connection channel holds the group.
    private(set) var parent: (any Channel)?

    /// The stream table the channels' streams run on. Set once, after the table is made.
    var table: QUICStreamTable<QUICStreamChannels>?

    /// Runs the inbound stream initializer. Held strongly until the table closes, as `parent`.
    var delegate: (any QUICStreamChannelGroupDelegate)?

    /// The `autoRead` option stream channels start with: on, as on the `SwiftNetwork` driver.
    var autoRead = true

    /// Channels with events a drain recorded, which ``deliver()`` fires.
    private var pending: [QUICStreamChannel]

    init(
        parent: any Channel,
        role: Role,
        localAddress: SocketAddress?,
        remoteAddress: SendableAddress
    ) {
        self.eventLoop = parent.eventLoop
        self.allocator = parent.allocator
        self.parent = parent
        self.role = role
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
        self.pending = []
    }

    /// Queues `channel`'s recorded events for ``deliver()``.
    func enqueue(_ channel: QUICStreamChannel) {
        self.pending.append(channel)
    }

    /// Fires the events the last drains recorded, in the order the streams were visited. Call it
    /// once the drain returned, never during one: the pipelines run user code, which may open,
    /// close and write to streams, or close the connection.
    func deliver() {
        while !self.pending.isEmpty {
            let channels = self.pending
            self.pending.removeAll(keepingCapacity: true)
            for channel in channels {
                channel.deliverRecordedEvents()
            }
        }
    }

    /// The table closed, with every stream: drop what holds the connection channel. The stream
    /// channels keep their parent.
    func tableClosed() {
        self.delegate = nil
        self.parent = nil
    }

    /// Opens an outbound stream of `type`, whose channel `promise` gets once `initializer` set it
    /// up; queued until the peer allows another stream (RFC 9000 Section 4.6).
    func createOutboundStream(
        type: QUICStreamType,
        promise: EventLoopPromise<any Channel>,
        initializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        self.eventLoop.assertInEventLoop()
        guard let table = self.table, !table.isClosed else {
            promise.fail(ChannelError.ioOnClosedChannel)
            return
        }
        let channel = QUICStreamChannel(outboundWith: initializer, promise: promise, group: self)
        do {
            let handle = try table.open(type, state: QUICStreamChannels.StreamState(channel: channel))
            channel.attach(handle: handle)
        } catch {
            promise.fail(error)
        }
    }

    /// Fires `ChannelShouldQuiesceEvent` on every stream channel.
    func quiesce() {
        var channels: [QUICStreamChannel] = []
        self.table?.forEachStreamState { state in
            channels.append(state.channel)
        }
        for channel in channels {
            channel.quiesce()
        }
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStreamChannelGroup: Sendable {}
