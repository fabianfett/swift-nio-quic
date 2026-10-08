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

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// What a stream table needs to attach a locally-opened stream to the stack.
@available(anyAppleOS 26, *)
struct QUICStreamOpener {
    /// The connection's stream listener.
    let listener: BaseStreamListenerLinkage

    /// The stack's connection instance.
    let connection: InstanceIdentifier

    let context: NetworkContext

    /// Attaches a new outbound flow for a stream.
    ///
    /// Does not return the new flow's linkage — `invokeAttachUpperProtocolToNewFlow` delivers it
    /// via a synchronous callback to `upperProtocol.invokeAttachLowerProtocol` instead, before it
    /// returns. Callers read it from wherever that callback stashed it.
    func attach(
        isUnidirectional: Bool,
        upperProtocol: BaseInboundStreamLinkage
    ) throws(NetworkError) {
        var parameters = SwiftNetwork.Parameters()
        parameters.context = self.context
        let path = SwiftNetwork.PathProperties(parameters: parameters)

        let options = QUICStreamProtocol.options()
        options.isUnidirectional = isUnidirectional
        options.setProtocolInstance(self.connection)
        parameters.defaultStack.prepend(applicationProtocol: options)

        try self.listener.invokeAttachUpperProtocolToNewFlow(
            upperProtocol,
            remote: nil,
            local: nil,
            parameters: parameters,
            path: path
        )
    }
}
