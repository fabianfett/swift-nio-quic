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
import NIOCore
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// Implementation of `QUICDatagramProtocol` backed by a SwiftNetwork QUIC datagram flow.
///
/// Conforms to SwiftNetwork's `ExternalInboundDatagramLinkage` to receive inbound datagram events
/// and be addressable as a protocol instance; the connection which owns it talks to it only
/// through `QUICDatagramProtocol`.
@available(anyAppleOS 26, *)
final class QUICDatagramTransport<Consumer: QUICStreamConsumer & ~Copyable>: ExternalInboundDatagramLinkage {

    internal var identifier: InstanceIdentifier
    internal var eventManager = ProtocolEventManager()
    internal var lower: BaseOutboundDatagramLinkage = .init()
    internal var context: SwiftNetwork.NetworkContext

    internal var logPrefix: String
    internal let logger: Logger

    private var bufferedDatagrams: TinyArray<ByteBuffer> = []
    private var reader: SwiftNetworkQUICConnection<Consumer>?
    private let callouts: QUICCallouts

    init(
        role: Role,
        logger: Logger,
        context: NetworkContext,
        callouts: QUICCallouts,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.logPrefix = "[\(role.description)][DatagramTransport]"
        self.logger = logger
        self.context = context
        self.callouts = callouts
        self.identifier = InstanceIdentifier(eventManager: &self.eventManager, in: &eventContext)
    }

    func setFlowLinkage(_ linkage: BaseOutboundDatagramLinkage) {
        self.lower = linkage
    }

    func log(_ message: @autoclosure () -> String) {
        #if DEBUG
        self.logger.trace("\(self.logPrefix) \(message())")
        #endif
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport: ProtocolInstance where Consumer: ~Copyable {}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport: QUICDatagramProtocol where Consumer: ~Copyable {
    func write(datagram: NIOCore.ByteBuffer) -> Bool {
        self.bufferedDatagrams.append(datagram)
        return true
    }

    func flush(in eventContext: inout NetworkContext.EventContext) {
        guard self.bufferedDatagrams.count > 0, lower.protocolIsConnected(in: &eventContext) else {
            return
        }

        do {
            self.log("writing \(self.bufferedDatagrams.count) QUIC datagrams")
            var frameArray = FrameArray(capacity: self.bufferedDatagrams.count)
            for var datagram in self.bufferedDatagrams {
                datagram.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
                    frameArray.add(frame: Frame(customBuffer: buffer, owner: owner))
                }
            }
            try lower.invokeSendDatagrams(frameArray, from: self.identifier, in: &eventContext)
            self.bufferedDatagrams.removeAll(where: { _ in true })
        } catch {
            self.logger.error("QUIC send datagrams: \(error)")
            if let reader = self.reader {
                self.callouts.add(.connection(reader, .datagramError(error)), in: &eventContext)
            }
        }
    }

    func close(in eventContext: inout NetworkContext.EventContext) {
        if !self.bufferedDatagrams.isEmpty {
            self.logger.debug("\(self.logPrefix) dropping \(self.bufferedDatagrams.count) unflushed datagrams on close")
        }

        do throws(NetworkError) {
            try lower.invokeDetach(for: self.identifier, in: &eventContext)
        } catch {
            self.logger.error("datagram flow detach failed: \(error)")
        }
        self.unregisterEventManager(in: &eventContext)

        self.reader = nil
    }

    func setReader(connection: SwiftNetworkQUICConnection<Consumer>) {
        self.reader = connection
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport where Consumer: ~Copyable {
    func handleInboundDataAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let reader = self.reader else {
            self.log("handle inbound data, but no reader set")
            return
        }

        do throws(NetworkError) {
            while var frames = try lower.invokeReceiveDatagrams(
                maximumDatagramCount: .max,
                for: self.identifier,
                in: &eventContext
            ), frames.count > 0 {
                frames.iterateMutableFrames { frame in
                    frame.span?.withUnsafeBufferPointer { spanBuffer in
                        var datagram = ByteBuffer()
                        datagram.writeWithUnsafeMutableBytes(minimumWritableBytes: spanBuffer.count) { buffer in
                            buffer.copyMemory(from: UnsafeRawBufferPointer(spanBuffer))
                            return spanBuffer.count
                        }
                        self.callouts.add(.connection(reader, .datagramRead(datagram)), in: &eventContext)
                    }
                    frame.finalize(success: true)
                    return true
                }
            }
        } catch {
            self.logger.error("QUIC datagram receive error: \(error)")
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport where Consumer: ~Copyable {
    /// Receives the datagram flow's linkage, delivered synchronously from inside
    /// `attachDatagramFlow`'s call to `invokeAttachUpperProtocolToNewFlow`, and completes the
    /// pairing: an external upper makes the second leg of the attach itself, which is what sets
    /// the flow up in QUIC, including its usable datagram size.
    func invokeAttachLowerProtocol(
        _ lowerProtocol: BaseOutboundDatagramLinkage,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        self.setFlowLinkage(lowerProtocol)
        try lowerProtocol.invokeAttachUpperProtocol(
            BaseInboundDatagramLinkage(external: self),
            remote: remote,
            local: local,
            parameters: parameters,
            path: path
        )
    }

    func handleOutboundRoomAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        // nop
    }

    func handleConnectedEvent(for instance: InstanceIdentifier, in eventContext: inout NetworkContext.EventContext) {
        log("received connected event")
    }

    func handleDisconnectedEvent(
        error: SwiftNetwork.NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("received disconnected event, error \(error.debugDescription)")
    }

    func handleNetworkProtocolEvent(
        event: SwiftNetwork.NetworkProtocolEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        // nop
    }
}
