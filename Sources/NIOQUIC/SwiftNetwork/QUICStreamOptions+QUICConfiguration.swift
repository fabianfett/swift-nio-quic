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
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS

@available(anyAppleOS 26, *)
extension QUICProtocol {
    static func options(
        from config: QUICConfiguration
    ) throws -> ProtocolOptions<QUICStreamProtocol> {
        let options = Self.options()

        options.connectionOptions.initialMaxData = UInt64(config.initialMaxData)
        options.connectionOptions.initialMaxStreamDataBidirectionalLocal = UInt64(config.initialMaxStreamDataBidiLocal)
        options.connectionOptions.initialMaxStreamDataBidirectionalRemote = UInt64(
            config.initialMaxStreamDataBidiRemote
        )
        options.connectionOptions.initialMaxStreamDataUnidirectional = UInt64(config.initialMaxStreamDataUni)
        options.connectionOptions.initialMaxStreamsBidirectional = UInt64(config.initialMaxStreamsBidi)
        options.connectionOptions.maximumConcurrentBidirectionalStreams = config.initialMaxStreamsBidi
        options.connectionOptions.initialMaxStreamsUnidirectional = UInt64(config.initialMaxStreamsUni)
        options.connectionOptions.maximumConcurrentUnidirectionalStreams = config.initialMaxStreamsUni
        options.connectionOptions.maxDatagramFrameSize = UInt16(clamping: config.maxDatagramFrameSize)

        guard let perProtocolOptions = options.perProtocolOptions else {
            throw QUICError.invalidConfiguration
        }

        perProtocolOptions.quicConnectionOptions.idleTimeout = NetworkDuration(duration: config.maxIdleTimeout)
        perProtocolOptions.quicConnectionOptions.initialPacketSize = UInt16(
            clamping: config.initialPacketSize.size(forClientInitial: nil)
        )

        if let qLogConfiguration = config.qLogConfiguration {
            perProtocolOptions.quicConnectionOptions.qlogConfiguration = SwiftNetwork.QLogConfiguration(
                logTitle: qLogConfiguration.topic.isEmpty ? nil : qLogConfiguration.topic,
                logDescription: qLogConfiguration.description.isEmpty ? nil : qLogConfiguration.description,
                logPath: qLogConfiguration.path
            )
        }

        return options
    }
}

/// SwiftNetwork's TLS protocol options wrap the TLS options both drivers build.
@available(anyAppleOS 26, *)
extension TLSProtocol.Options {
    static func serverOptions(
        from config: QUICConfiguration,
        authConfig: AuthenticationConfiguration,
        authenticator: Authenticator?,
        serverName: String
    ) throws -> Self {
        var options = SwiftTLSProtocol.Options()
        options.tlsOptions = try SwiftTLSOptions.serverOptions(
            from: config,
            authConfig: authConfig,
            authenticator: authenticator,
            serverName: serverName
        )
        return options
    }

    static func clientOptions(
        from config: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        serverName: String
    ) throws -> Self {
        var options = SwiftTLSProtocol.Options()
        options.tlsOptions = try SwiftTLSOptions.clientOptions(
            from: config,
            asyncVerifier: asyncVerifier,
            serverName: serverName
        )
        return options
    }
}
