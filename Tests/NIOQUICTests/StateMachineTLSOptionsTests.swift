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
import Foundation
import NIOCore
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS
import Testing

// NIOQUIC's extension of SwiftNetwork's TLS options is in the SPI group of the type it extends.
@_spi(ProtocolProvider) @testable import NIOQUIC

/// The state machine connections in these tests are never created.
@available(anyAppleOS 26, *)
private struct NoConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) {}

    mutating func processStreams(_ streams: inout NIOQUIC.QUICStreamIterator<Self>) {
        while streams.next() != nil {}
    }
}

/// What a handshake depends on in `SwiftTLSOptions`, which isn't `Equatable`.
@available(anyAppleOS 26, *)
private struct TLSOptionsSummary: Equatable {
    var serverName: String?
    var applicationProtocols: [String]?
    var keyExchangeGroup: SwiftTLSOptions.KeyExchangeGroup
    var supportedCipherSuites: [SwiftTLSOptions.CipherSuite]
    var quicTransportParameters: [UInt8]?
    var trustedRawPublicKeyCertificates: [[UInt8]]?
    var trustedRawPublicKeyP256PublicKeyCount: Int?
    var hasPrivateKey: Bool
    var rawPrivateKey: [UInt8]?
    var enableEarlyData: Bool
    var sessionState: [UInt8]?
    var newSessionTicketRequestCount: UInt8
    var resumedSessionTicketRequestCount: UInt8
    var clientAuthRequired: Bool
    var hasExternalPSK: Bool
    var verifierCertificateTypes: [CertificateType]?
    var authenticatorCertificateTypes: [CertificateType]?

    init(_ options: SwiftTLSOptions) {
        self.serverName = options.serverName
        self.applicationProtocols = options.applicationProtocols
        self.keyExchangeGroup = options.keyExchangeGroup
        self.supportedCipherSuites = options.supportedCipherSuites
        self.quicTransportParameters = options.quicTransportParameters
        self.trustedRawPublicKeyCertificates = options.trustedRawPublicKeyCertificates
        self.trustedRawPublicKeyP256PublicKeyCount = options.trustedRawPublicKeyP256PublicKeys?.count
        self.hasPrivateKey = options.privateKey != nil
        self.rawPrivateKey = options.rawPrivateKey
        self.enableEarlyData = options.enableEarlyData
        self.sessionState = options.sessionState
        self.newSessionTicketRequestCount = options.newSessionTicketRequestCount
        self.resumedSessionTicketRequestCount = options.resumedSessionTicketRequestCount
        self.clientAuthRequired = options.clientAuthRequired
        self.hasExternalPSK = options.externalPSK != nil
        self.verifierCertificateTypes = options.asyncVerifier?.availableCertificateTypes
        self.authenticatorCertificateTypes = options.asyncAuthenticator?.supportedCertificateTypes
    }
}

/// The state machine driver hands the core the TLS options the SwiftNetwork driver wraps in its
/// TLS protocol options, so both drivers run the same handshake.
@Suite(.timeLimit(.minutes(1)))
struct StateMachineTLSOptionsTests {
    @available(anyAppleOS 26, *)
    private typealias Connection = StateMachineQUICConnection<NoConsumer>

    @available(anyAppleOS 26, *)
    private static func remoteAddress() throws -> SocketAddress {
        try SocketAddress(ipAddress: "127.0.0.1", port: 4433)
    }

    @available(anyAppleOS 26, *)
    private static func p256PublicKey() throws -> [UInt8] {
        Array(try P256.Signing.PublicKey.fromDERFile(p256PublicKeyPath).derRepresentation)
    }

    /// The TLS options of a client on either driver.
    @available(anyAppleOS 26, *)
    private static func clientOptions(
        _ configuration: QUICConfiguration,
        serverName: String,
        asyncVerifier: NIOQUIC.AsyncVerifier?
    ) throws -> (stateMachine: QUICConnectionCore.Configuration, swiftNetwork: TLSProtocol.Options) {
        let stateMachine = try Connection.clientCoreConfiguration(
            configuration,
            serverName: serverName,
            asyncVerifier: asyncVerifier,
            remoteAddress: try Self.remoteAddress()
        )
        let swiftNetwork = try TLSProtocol.Options.clientOptions(
            from: configuration,
            asyncVerifier: asyncVerifier,
            serverName: serverName
        )
        return (stateMachine, swiftNetwork)
    }

    /// The TLS options of a server on either driver.
    @available(anyAppleOS 26, *)
    private static func serverOptions(
        _ configuration: QUICConfiguration,
        authenticator: Authenticator?
    ) throws -> (stateMachine: QUICConnectionCore.Configuration, swiftNetwork: TLSProtocol.Options) {
        let stateMachine = try Connection.serverCoreConfiguration(
            configuration,
            sourceConnectionID: NIOQUIC.QUICConnectionID(bytes: [1, 2, 3, 4, 5, 6, 7, 8].span),
            statelessResetTokenGenerator: .defaultWithAutoGeneratedKey,
            authenticator: authenticator,
            remoteAddress: try Self.remoteAddress()
        )
        let swiftNetwork = try TLSProtocol.Options.serverOptions(
            from: configuration,
            authConfig: try #require(configuration.authenticationConfiguration),
            authenticator: authenticator,
            serverName: try #require(configuration.serverName)
        )
        return (stateMachine, swiftNetwork)
    }

    /// What every connection's TLS options have: the configured ALPN, server name and key
    /// exchange group, SwiftTLS's default cipher suite, no 0-RTT, and no transport parameters,
    /// which the connection adds.
    @available(anyAppleOS 26, *)
    private static func expectCommonOptions(
        _ options: (stateMachine: QUICConnectionCore.Configuration, swiftNetwork: TLSProtocol.Options),
        applicationProtocols: [String],
        serverName: String,
        keyExchangeGroup: SwiftTLSOptions.KeyExchangeGroup,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        let tls = options.stateMachine.tls
        #expect(
            TLSOptionsSummary(tls) == TLSOptionsSummary(options.swiftNetwork.tlsOptions),
            sourceLocation: sourceLocation
        )
        #expect(tls.applicationProtocols == applicationProtocols, sourceLocation: sourceLocation)
        #expect(tls.serverName == serverName, sourceLocation: sourceLocation)
        #expect(options.swiftNetwork.serverName == serverName, sourceLocation: sourceLocation)
        #expect(tls.keyExchangeGroup == keyExchangeGroup, sourceLocation: sourceLocation)
        #expect(tls.supportedCipherSuites == [.AES256GCM_SHA384], sourceLocation: sourceLocation)
        #expect(tls.quicTransportParameters == nil, sourceLocation: sourceLocation)
        #expect(!tls.enableEarlyData, sourceLocation: sourceLocation)
        #expect(tls.sessionState == nil, sourceLocation: sourceLocation)
        #expect(!tls.clientAuthRequired, sourceLocation: sourceLocation)
        #expect(tls.externalPSK == nil, sourceLocation: sourceLocation)
        #expect(options.stateMachine.resumedTransportParameters == nil, sourceLocation: sourceLocation)
        #expect(options.swiftNetwork.resumedQUICTransportParameters == nil, sourceLocation: sourceLocation)
    }

    /// The configuration's key exchange group is x25519 by default, which SwiftNetwork's TLS
    /// options default to but `SwiftTLSOptions()` doesn't.
    @available(anyAppleOS 26, *)
    @Test
    func aClientWithRawPublicKeysNegotiatesTheConfiguredGroup() throws {
        let groups: [(NIOQUIC.KeyExchangeGroup?, SwiftTLSOptions.KeyExchangeGroup)] = [
            (nil, .x25519), (.secp256, .secp256), (.secp384, .secp384), (.x25519, .x25519),
            (.x25519MLKEM768, .x25519MLKEM768),
        ]
        for (group, expected) in groups {
            var configuration = QUICConfiguration.forClient(
                verificationConfiguration: .rawPublicKeys(publicKeyFilePath: p256PublicKeyPath),
                applicationProtocols: ["h3", "echo"]
            )
            if let group {
                configuration.keyExchangeGroup = group
            }
            let options = try Self.clientOptions(configuration, serverName: "example.com", asyncVerifier: nil)

            Self.expectCommonOptions(
                options,
                applicationProtocols: ["h3", "echo"],
                serverName: "example.com",
                keyExchangeGroup: expected
            )
            let tls = options.stateMachine.tls
            #expect(tls.trustedRawPublicKeyCertificates == [try Self.p256PublicKey()])
            #expect(tls.rawPrivateKey == nil)
            #expect(tls.privateKey == nil)
            #expect(tls.asyncVerifier == nil)
            #expect(tls.asyncAuthenticator == nil)
        }
    }

    /// A client verifying certificates runs the verifier's callback for the server name.
    @available(anyAppleOS 26, *)
    @Test(arguments: [CertificateVerification.noVerification, .fullVerification])
    func aClientWithCertificatesUsesTheVerifier(verification: CertificateVerification) throws {
        let configuration = QUICConfiguration.forClient(
            verificationConfiguration: .x509Certificates(trustRootsFilePath: nil),
            applicationProtocols: ["h3"]
        )
        let verifier = NIOQUIC.AsyncVerifier(certificateVerification: verification)
        let options = try Self.clientOptions(configuration, serverName: "example.com", asyncVerifier: verifier)

        Self.expectCommonOptions(
            options,
            applicationProtocols: ["h3"],
            serverName: "example.com",
            keyExchangeGroup: .x25519
        )
        let tls = options.stateMachine.tls
        #expect(tls.trustedRawPublicKeyCertificates == nil)
        #expect(tls.asyncAuthenticator == nil)
        let stateMachineVerifier = try #require(tls.asyncVerifier)
        let swiftNetworkVerifier = try #require(options.swiftNetwork.tlsOptions.asyncVerifier)
        #expect(stateMachineVerifier.availableCertificateTypes == [.x509])

        // Without verification anything goes; otherwise the verifier refuses what isn't X.509
        // right away, before it would wait for its `run()`.
        let info = VerificationInfo(
            certificates: CertificateList(type: .rawPublicKey, entries: []),
            signatureAlgorithm: 0,
            signature: Data(),
            transcriptHash: Data()
        )
        let expected: SwiftTLS.VerificationResult =
            verification == .noVerification ? .valid : .invalid(reason: "unsupported certificate type")
        #expect(stateMachineVerifier.verifyHandshake(info) == expected)
        #expect(swiftNetworkVerifier.verifyHandshake(info) == expected)
    }

    /// Without a verification configuration a client trusts nothing; one verifying certificates
    /// needs a verifier, and every client a server name.
    @available(anyAppleOS 26, *)
    @Test
    func aClientNeedsWhatItsVerificationConfigurationNames() throws {
        var configuration = QUICConfiguration.forClient(
            verificationConfiguration: .x509Certificates(trustRootsFilePath: nil),
            applicationProtocols: ["h3"]
        )
        #expect(throws: QUICError.tlsConfigurationIncomplete) {
            _ = try Self.clientOptions(configuration, serverName: "example.com", asyncVerifier: nil)
        }
        #expect(throws: QUICError.tlsConfigurationIncomplete) {
            _ = try Connection.clientCoreConfiguration(
                configuration,
                serverName: nil,
                asyncVerifier: NIOQUIC.AsyncVerifier(),
                remoteAddress: try Self.remoteAddress()
            )
        }

        configuration.verificationConfiguration = nil
        let options = try Self.clientOptions(configuration, serverName: "example.com", asyncVerifier: nil)
        Self.expectCommonOptions(
            options,
            applicationProtocols: ["h3"],
            serverName: "example.com",
            keyExchangeGroup: .x25519
        )
        let tls = options.stateMachine.tls
        #expect(tls.trustedRawPublicKeyCertificates == nil)
        #expect(tls.asyncVerifier == nil)
    }

    /// A server with raw public keys presents its private key and trusts its public key.
    @available(anyAppleOS 26, *)
    @Test
    func aServerWithRawPublicKeysHasItsKeys() throws {
        var configuration = QUICConfiguration.forServer(
            serverName: "example.com",
            authenticationConfiguration: .rawPublicKeys(
                publicKeyFilePath: p256PublicKeyPath,
                privateKeyFilePath: p256PrivateKeyPath
            ),
            applicationProtocols: ["h3", "echo"]
        )
        configuration.keyExchangeGroup = .secp256
        let options = try Self.serverOptions(configuration, authenticator: nil)

        Self.expectCommonOptions(
            options,
            applicationProtocols: ["h3", "echo"],
            serverName: "example.com",
            keyExchangeGroup: .secp256
        )
        let tls = options.stateMachine.tls
        #expect(tls.trustedRawPublicKeyCertificates == [try Self.p256PublicKey()])
        let privateKey = try P256.Signing.PrivateKey.fromDERFile(p256PrivateKeyPath)
        #expect(tls.rawPrivateKey == Array(privateKey.rawRepresentation))
        #expect(tls.asyncAuthenticator == nil)
        #expect(tls.asyncVerifier == nil)
    }

    /// A server with certificates authenticates through the authenticator, which it needs.
    @available(anyAppleOS 26, *)
    @Test
    func aServerWithCertificatesUsesTheAuthenticator() throws {
        let configuration = QUICConfiguration.forServer(
            serverName: "example.com",
            authenticationConfiguration: .x509Certificates(
                certificateChainFilePath: StateMachineTLSOptionsTests.certificatePath,
                privateKeyFilePath: StateMachineTLSOptionsTests.certificateKeyPath
            ),
            applicationProtocols: ["h3"]
        )
        #expect(throws: QUICError.tlsConfigurationIncomplete) {
            _ = try Self.serverOptions(configuration, authenticator: nil)
        }

        let authenticator = try Authenticator(
            certificateFilePath: StateMachineTLSOptionsTests.certificatePath,
            privateKeyFilePath: StateMachineTLSOptionsTests.certificateKeyPath
        )
        let options = try Self.serverOptions(configuration, authenticator: authenticator)

        Self.expectCommonOptions(
            options,
            applicationProtocols: ["h3"],
            serverName: "example.com",
            keyExchangeGroup: .x25519
        )
        let tls = options.stateMachine.tls
        #expect(tls.asyncAuthenticator?.supportedCertificateTypes == [.x509])
        #expect(tls.trustedRawPublicKeyCertificates == nil)
        #expect(tls.rawPrivateKey == nil)
        #expect(tls.asyncVerifier == nil)
    }

    private static let certificatePath = Bundle.module.url(forResource: "testcert", withExtension: "pem")!.path
    private static let certificateKeyPath = Bundle.module.url(forResource: "testkey", withExtension: "pem")!.path
}
