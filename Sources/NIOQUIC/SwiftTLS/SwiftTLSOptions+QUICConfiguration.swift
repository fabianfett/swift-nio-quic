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

import Crypto
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS

/// The TLS options of a QUIC connection's handshake, for either driver: the state machine driver
/// hands them to the connection core, the SwiftNetwork driver wraps them in its TLS protocol
/// options.
///
/// The connection sets `quicTransportParameters` itself, so these leave them out.
@available(anyAppleOS 26, *)
extension SwiftTLSOptions {
    /// The TLS options of a server, which authenticates with the certificates `authenticator`
    /// provides, or with the raw public key in `authConfig`.
    ///
    /// - Throws: ``QUICError/tlsConfigurationIncomplete`` if `authConfig` asks for certificates
    ///   but there is no `authenticator`, or the error reading a raw key file.
    static func serverOptions(
        from config: QUICConfiguration,
        authConfig: AuthenticationConfiguration,
        authenticator: Authenticator?,
        serverName: String
    ) throws -> Self {
        var options = Self(quicConfiguration: config, serverName: serverName)

        switch authConfig {
        case .x509Certificates:
            guard let authenticator else {
                throw QUICError.tlsConfigurationIncomplete
            }
            options.asyncAuthenticator = .init(
                supportedCertificateTypes: [.x509],
                getCertificateChain: authenticator.produceCertificates(info:),
                signTranscriptHash: authenticator.provideSignature(info:)
            )

        case .rawPublicKeys(let publicKeyFilePath, let privateKeyFilePath):
            let publicKey = try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath)
            options.trustedRawPublicKeyCertificates = [Array(publicKey.derRepresentation)]

            let privateKey = try P256.Signing.PrivateKey.fromDERFile(privateKeyFilePath)
            options.rawPrivateKey = Array(privateKey.rawRepresentation)
        }

        return options
    }

    /// The TLS options of a client for a server called `serverName`, which it verifies with
    /// `asyncVerifier` or the raw public key in the configuration's `verificationConfiguration`.
    /// Without a `verificationConfiguration` the client trusts no certificate.
    ///
    /// - Throws: ``QUICError/tlsConfigurationIncomplete`` if the configuration asks for
    ///   certificates but there is no `asyncVerifier`, or the error reading a raw key file.
    static func clientOptions(
        from config: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        serverName: String
    ) throws -> Self {
        var options = Self(quicConfiguration: config, serverName: serverName)

        if let verificationConfiguration = config.verificationConfiguration {
            switch verificationConfiguration {
            case .rawPublicKeys(let publicKeyFilePath):
                let publicKey = try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath)
                options.trustedRawPublicKeyCertificates = [Array(publicKey.derRepresentation)]

            case .x509Certificates:
                guard let asyncVerifier else {
                    throw QUICError.tlsConfigurationIncomplete
                }
                options.asyncVerifier = .init(
                    availableCertificateTypes: [.x509],
                    verificationCallback: asyncVerifier.makeCallback(hostname: serverName)
                )
            }
        }

        return options
    }

    /// The options both ends share.
    private init(quicConfiguration config: QUICConfiguration, serverName: String) {
        self.init()
        self.applicationProtocols = config.applicationProtocols
        self.serverName = serverName
        // `SwiftTLSOptions()` negotiates secp384r1, SwiftNetwork's TLS options x25519; the
        // configuration's group, x25519 by default, applies to both drivers.
        self.keyExchangeGroup = config.keyExchangeGroup.swiftTLSKeyExchangeGroup
    }
}
