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
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Android)
import Android
#elseif canImport(Musl)
import Musl
#endif

/// The connection a ``QUICChannelNewFlowHandler`` reports to.
@available(anyAppleOS 26, *)
protocol QUICNewFlowOwner: AnyObject {
    func connected(in eventContext: inout NetworkContext.EventContext)
    func disconnected(error: NetworkError?, in eventContext: inout NetworkContext.EventContext)
    func newInboundStream(_ handler: QUICChannelStreamHandler)
    func associateConnectionID(_ cid: QUICConnectionID, in eventContext: inout NetworkContext.EventContext)
    func retireConnectionID(_ cid: QUICConnectionID, in eventContext: inout NetworkContext.EventContext)
    func pathValidated(remote: AddressEndpoint)
    func pathUnreachable(remote: AddressEndpoint, in eventContext: inout NetworkContext.EventContext)
}

/// How ``QUICInboundStreamTable/attachInboundFlow(lowerProtocol:flowInstance:streamID:in:)`` went.
@available(anyAppleOS 26, *)
enum QUICInboundFlowAttachment {
    case attached
    /// The table is closed, so nothing would close the stream.
    case declinedTableClosed
    case failed(NetworkError)
}

/// The stream table a ``QUICChannelNewFlowHandler`` attaches inbound flows to.
@available(anyAppleOS 26, *)
protocol QUICInboundStreamTable: AnyObject {
    /// Gives the inbound flow `flowInstance` a slot, attaches it and connects it.
    func attachInboundFlow(
        lowerProtocol: BaseStreamListenerLinkage,
        flowInstance: InstanceIdentifier,
        streamID: QUICStreamID,
        in eventContext: inout NetworkContext.EventContext
    ) -> QUICInboundFlowAttachment
}

/// `QUICChannelNewFlowHandler` is responsible for dealing with new SwiftNetwork 'flows' initiated by the other end of the connection.
/// A flow in this case is a new stream of data that is registered with the QUIC stack and is represented here by `QUICChannelStreamHandler`.
/// The `QUICChannelNewFlowHandler` registered with the SwiftNetwork `QUICConnection` as a new flow handler.
/// This object deals with creating and linking the objects describing a new flow, it creates a new `QUICChannelStreamHandler`
/// for each new flow, registers it and keeps track of it.
///
/// Not generic over the consumer: SwiftNetwork calls into it for every inbound stream, and the connection enters
/// the stack as it on every read loop. It reaches the connection and its stream table through
/// ``QUICNewFlowOwner`` and ``QUICInboundStreamTable``.
@available(anyAppleOS 26, *)
final class QUICChannelNewFlowHandler: ExternalInboundStreamFlowLinkage {

    internal typealias LowerProtocol = BaseStreamListenerLinkage
    typealias UpperStreamHandlerType = QUICChannelStreamHandler

    internal var eventManager = ProtocolEventManager()
    internal var logPrefix: String
    internal var identifier: InstanceIdentifier = InstanceIdentifier()
    internal var context: SwiftNetwork.NetworkContext

    let local: Endpoint
    let remote: Endpoint
    let parameters: Parameters
    let path: PathProperties
    let role: Role
    let logger: Logger
    let remoteAddress: SendableAddress
    let localAddress: SocketAddress
    private let callouts: QUICCallouts

    /// The connection channel which is the parent of inbound stream channels created by this handler.
    /// Set via ``setConnectionChannel(_:)`` once the connection channel has been created.
    private var connectionChannel: (any Channel)?

    private var connectionView: (any QUICNewFlowOwner)!
    private var lowerProtocol: BaseStreamListenerLinkage?
    private var datagramListener: BaseDatagramListenerLinkage

    var keepAliveInterval: Duration?

    /// The stream table inbound flows attach to, or nil if inbound streams become
    /// child channels.
    var streamTable: (any QUICInboundStreamTable)?

    internal init?(
        local: Endpoint,
        remote: Endpoint,
        parameters: Parameters,
        path: PathProperties,
        logger: Logger,
        remoteAddress: SendableAddress,
        localAddress: SocketAddress,
        role: Role,
        streamListenerProtocol: BaseStreamListenerLinkage,
        datagramListenerProtocol: BaseDatagramListenerLinkage,
        callouts: QUICCallouts,
        keepAliveInterval: Duration? = nil
    ) {
        #if DEBUG
        assert(
            !callouts.isInsideStack,
            "QUICChannelNewFlowHandler attaches on construction, which requires that no event context is held"
        )
        #endif
        self.local = local
        self.remote = remote
        self.parameters = parameters
        self.path = path
        self.role = role
        self.logger = logger
        self.remoteAddress = remoteAddress
        self.context = parameters.context
        self.keepAliveInterval = keepAliveInterval
        self.logPrefix = "[\(self.role.description)][NewFlowHandler]"
        self.localAddress = localAddress
        self.datagramListener = datagramListenerProtocol
        self.callouts = callouts
        self.identifier = InstanceIdentifier(context: self.context, eventManager: &self.eventManager)
        do throws(NetworkError) {
            try callouts.callFramework { () throws(NetworkError) in
                try streamListenerProtocol.invokeAttachUpperProtocol(
                    BaseInboundStreamFlowLinkage(external: self),
                    remote: remote,
                    local: local,
                    parameters: parameters,
                    path: path
                )
            }
        } catch {
            self.callouts.enter(as: self) { eventContext in
                self.unregisterEventManager(in: &eventContext)
            }
            return nil
        }
        self.lowerProtocol = streamListenerProtocol
    }

    // Set the parent (connection) channel that inbound streams will use as their parent.
    // Called once the connection child channel has been created.
    func setConnectionChannel(_ channel: any Channel) {
        self.connectionChannel = channel
    }

    // Drop the strong reference to the connection channel, breaking the channel <-> connection
    // retain cycle once the channel has gone inactive.
    func clearConnectionChannel() {
        self.connectionChannel = nil
    }

    /// Local logging function to debug the datapath
    ///
    /// This layer adds the context and fetches the message only if the debug flags are enabled.
    ///
    /// - Parameters:
    ///     - logMessage: The logMessage that is fetched by an autoclosure.  For performance reasons we could gate this behind a flag.
    func log(_ logMessage: @autoclosure () -> String) {
        #if DEBUG
        let message = logMessage()
        self.logger.trace("\(self.logPrefix) \(message)")
        #endif
    }

    // Start the new flow handler
    func start(
        _ view: any QUICNewFlowOwner,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("start")
        self.connectionView = view
        self.lowerProtocol?.invokeConnect(for: self.identifier, in: &eventContext)
    }

    // Fetch QUIC metadata via the main connection (listener) linkage.
    // `connectionMetadata.activeConnectionIDLimit` is the peer's advertised cap
    // on how many connection IDs we may issue (RFC 9000 §18.2).
    func getConnectionMetadata(in eventContext: inout NetworkContext.EventContext) -> ProtocolMetadata<QUICProtocol>? {
        self.lowerProtocol?.invokeGetMetadata(for: self.identifier, in: &eventContext)
            as? ProtocolMetadata<QUICProtocol>
    }

    // Stop the new flow handler
    func stop(error: NetworkError? = nil, in eventContext: inout NetworkContext.EventContext) {
        log("stop")
        self.lowerProtocol?.invokeDisconnect(error: error, for: self.identifier, in: &eventContext)
    }

    // Teardown the new flow handler
    internal func teardown(in eventContext: inout NetworkContext.EventContext) {
        do throws(NetworkError) {
            let lower = self.lowerProtocol.take()
            try lower?.invokeDetach(for: self.identifier, in: &eventContext)
        } catch {
            self.log("Failed to detach lower protocol: \(error)")
        }
        self.unregisterEventManager(in: &eventContext)
    }

    func outboundBatching(_ isEnabled: Bool, in eventContext: inout NetworkContext.EventContext) {
        let event: ApplicationEvent = isEnabled ? .outboundDataBatchStart : .outboundDataBatchEnd
        self.lowerProtocol?.invokeApplicationEvent(event: event, for: self.identifier, in: &eventContext)
    }

    // Request association of a new connection ID that our peer can use to contact us.
    func requestAssociationOfConnectionID(
        _ connectionID: QUICConnectionID,
        statelessResetToken: SwiftNetwork.QUICStatelessResetToken,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.log("New connection ID: '\(connectionID)'")
        let cid = SwiftNetwork.QUICConnectionID(connectionID)
        let quicEvent = QUICApplicationEvent.announceNewInboundConnectionID(
            cid,
            statelessResetToken: statelessResetToken
        )
        self.lowerProtocol?.invokeApplicationEvent(
            event: ApplicationEvent(quicEvent: quicEvent),
            for: self.identifier,
            in: &eventContext
        )
    }

    // Request retirement of a connection ID that we are using to address our peer.
    func requestsRetirementOfConnectionID(
        _ connectionID: QUICConnectionID,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.log("Retiring connection ID: '\(connectionID)'")
        let cid = SwiftNetwork.QUICConnectionID(connectionID)
        let quicEvent = QUICApplicationEvent.retireOutboundConnectionID(cid)
        self.lowerProtocol?.invokeApplicationEvent(
            event: ApplicationEvent(quicEvent: quicEvent),
            for: self.identifier,
            in: &eventContext
        )
    }

    /// Unused: nothing calls this witness on this type, so it only throws `ENOTSUP`.
    ///
    /// `init` attaches this handler to the stream listener with `invokeAttachUpperProtocol`, which
    /// calls nothing back, and keeps the listener as `lowerProtocol` itself. The stream table's
    /// linkages, the datagram transport and outbound stream handlers attach with
    /// `invokeAttachUpperProtocolToNewFlow` instead, which hands them the new flow through this
    /// witness, and theirs complete the attach.
    func invokeAttachLowerProtocol(
        _ lowerProtocol: BaseStreamListenerLinkage,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    // Received connected event
    func handleConnectedEvent(for instance: InstanceIdentifier, in eventContext: inout NetworkContext.EventContext) {
        log("connected received")
        self.connectionView.connected(in: &eventContext)
    }

    // Received disconnected event
    func handleDisconnectedEvent(
        error: SwiftNetwork.NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("received disconnected with error: \(String(describing: error))")
        self.connectionView.disconnected(error: error, in: &eventContext)
        self.connectionView = nil
    }

    func handleNetworkProtocolEvent(
        event: SwiftNetwork.NetworkProtocolEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.log("Network protocol event: \(event)")
        if let quicEvent = event.quicEvent {
            switch quicEvent {
            case .newInboundConnectionID(let connectionID):
                self.connectionView.associateConnectionID(NIOQUIC.QUICConnectionID(connectionID), in: &eventContext)
            case .retiredInboundConnectionID(let connectionID):
                self.connectionView.retireConnectionID(NIOQUIC.QUICConnectionID(connectionID), in: &eventContext)
            case .newOutboundConnectionID, .retiredOutboundConnectionID, .earlyDataRejected,
                .maxStreamsLimitBidirectionalUpdated, .maxStreamsLimitUnidirectionalUpdated,
                .receivedRemoteTransportParameters, .remoteBidirectionalStreamsBlocked,
                .remoteUnidirectionalStreamsBlocked,
                // The connection takes the last validated path as the active one, so it doesn't act on
                // these path events (yet).
                .pathCIDAssigned, .pathCurrent:
                break
            case .pathChanged:
                // SwiftNetwork announces a path as soon as it is attached. Not much to do at that point.
                break
            case .pathValidated(let info):
                self.connectionView.pathValidated(remote: info.remote)
            case .pathUnreachable(let info):
                self.connectionView.pathUnreachable(remote: info.remote, in: &eventContext)
            }
        }
    }

    // Receive a new inbound flow and either give it a slot in the stream table or
    // create a QUICChannelStreamHandler for it.
    internal func handleNewInboundFlowEvent(
        flowInstance: InstanceIdentifier,
        flowMetadata: AbstractProtocolMetadata?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("received new inbound flow")

        guard let lowerProtocol = self.lowerProtocol else {
            self.log("Dropping new inbound flow: lower protocol has been detached")
            return
        }

        guard let metadata = flowMetadata as? ProtocolMetadata<QUICProtocol>,
            let inputHandlerStreamID = metadata.streamID
        else {
            logger.error("Could not create new stream handler: invalid metadata")
            return
        }

        // Before the connection channel is required below: a connection using a stream
        // table has no child channel to parent the stream to.
        if let table = self.streamTable {
            let streamID = QUICStreamID(rawValue: inputHandlerStreamID)
            switch table.attachInboundFlow(
                lowerProtocol: lowerProtocol,
                flowInstance: flowInstance,
                streamID: streamID,
                in: &eventContext
            ) {
            case .attached:
                ()
            case .declinedTableClosed:
                // Nothing would close this stream any more: leave the flow to the connection's teardown.
                self.log("Declining inbound flow for stream \(streamID): the stream table is closed")
            case .failed(let error):
                self.log("Failed to attach new inbound flow: \(error)")
            }
        } else if let connectionChannel = self.connectionChannel {
            // Registers as it's created.
            let streamHandler = QUICChannelStreamHandler(
                role: self.role,
                parameters: self.parameters,
                streamID: QUICStreamID(rawValue: inputHandlerStreamID),
                logger: self.logger,
                remoteAddress: self.remoteAddress,
                localAddress: self.localAddress,
                connectionChannel: connectionChannel,
                callouts: self.callouts,
                keepAliveInterval: keepAliveInterval,
                in: &eventContext
            )

            do throws(NetworkError) {
                let linkage = try lowerProtocol.invokeAttachUpperProtocolToExistingFlow(
                    BaseInboundStreamLinkage(external: streamHandler),
                    existingFlowInstance: flowInstance
                )
                streamHandler.swiftNetworkStreamHandle = SwiftNetworkStreamHandle(
                    linkage: linkage,
                    identifier: streamHandler.identifier
                )
                streamHandler.setNewFlowMetadata(metadata)
                streamHandler.start(fromNewFlowHandler: true, in: &eventContext)
                self.connectionView.newInboundStream(streamHandler)

                // For new inbound flows, only set the keep-alive interval once for the connection.
                // That means only the first flow should send the interval into QUICChannelStreamHandler
                self.keepAliveInterval = nil
            } catch {
                self.log("Failed to attach new inbound flow: \(error)")
                // The attach bound nothing to it, and nothing else will tear it down.
                streamHandler.unregisterEventManager(in: &eventContext)
            }
        } else {
            fatalError("connection channel is not available")
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelNewFlowHandler: ProtocolInstance {}

@available(anyAppleOS 26, *)
extension QUICChannelNewFlowHandler {
    /// Attaches the connection's datagram flow.
    ///
    /// Must be called with no event context held: attaching a new flow acquires the context itself.
    ///
    /// The connection calls this from its `start`, after starting this handler and before any packet
    /// is processed, so that no early DATAGRAM frame finds no flow: QUIC would give that frame an
    /// inbound flow of its own, which nothing attaches to, and the datagram would be lost
    /// (`QUICConnection.processDatagramFrame`). QUIC recomputes the flow's usable datagram size when
    /// the peer's transport parameters arrive (`QUICConnection.setRemoteMaxDatagramFrameSize`). The
    /// handshake can't have completed yet, so the `invokeConnect` below leaves a connect request on
    /// the flow, which QUIC acts on when the handshake completes.
    ///
    /// - Returns: The transport for the attached flow, or `nil` if it could not be attached.
    func attachDatagramFlow<Consumer: QUICStreamConsumer & ~Copyable>(
        for consumer: Consumer.Type
    ) -> QUICDatagramTransport<Consumer>? {
        #if DEBUG
        assert(!self.callouts.isInsideStack, "Attaching a new flow requires that no event context is held")
        #endif
        let transport = self.callouts.enter(as: self) { eventContext in
            QUICDatagramTransport<Consumer>(
                role: self.role,
                logger: self.logger,
                context: self.context,
                callouts: self.callouts,
                in: &eventContext
            )
        }

        do {
            var swiftNetworkParameters = SwiftNetwork.Parameters()
            swiftNetworkParameters.context = self.context
            let swiftNetworkPath = SwiftNetwork.PathProperties(parameters: swiftNetworkParameters)
            let quicOptions = QUICStreamProtocol.options()
            if quicOptions.perProtocolOptions == nil {
                throw QUICError.invalidConfiguration
            }
            quicOptions.modifyPerProtocolOptions { $0.isDatagram = true }
            quicOptions.setProtocolInstance(self.datagramListener.identifier)
            swiftNetworkParameters.defaultStack.prepend(applicationProtocol: quicOptions)

            try self.callouts.callFramework { () throws(NetworkError) in
                try self.datagramListener.invokeAttachUpperProtocolToNewFlow(
                    BaseInboundDatagramLinkage(external: transport),
                    remote: nil,
                    local: nil,
                    parameters: swiftNetworkParameters,
                    path: swiftNetworkPath
                )
            }
        } catch {
            self.logger.error("\(self.logPrefix) Failed to attach QUIC datagram flow: \(error)")
            // Registered but never attached: nothing else will unregister it.
            self.callouts.enter(as: transport) { eventContext in
                transport.unregisterEventManager(in: &eventContext)
            }
            return nil
        }

        self.callouts.enter(as: transport) { eventContext in
            transport.lower.invokeConnect(for: transport.identifier, in: &eventContext)
        }
        return transport
    }
}
