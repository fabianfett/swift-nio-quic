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
@_spi(Essentials) @_spi(ProtocolProvider) import QUICStateMachine
import Testing

@testable import NIOQUIC

/// The state machine connections in these tests are never created.
@available(anyAppleOS 26, *)
private struct NoConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while streams.next() != nil {}
    }
}

@Suite(.timeLimit(.minutes(1)))
struct StateMachineCoreConfigurationTests {
    /// PMTUD searches up to the maximum payload plus the IP and UDP headers to the peer: 28 bytes
    /// over IPv4, also to an IPv4-mapped IPv6 address, and 48 over IPv6.
    @available(anyAppleOS 26, *)
    @Test(arguments: [
        ("127.0.0.1", 28), ("192.0.2.17", 28), ("::ffff:192.0.2.17", 28), ("::1", 48), ("2001:db8::1:2", 48),
        ("::192.0.2.1", 48), ("64:ff9b::192.0.2.1", 48),
    ])
    func probingCapsTheMTUAtTheMaximumPlusTheHeaders(ipAddress: String, headerSize: Int) throws {
        var configuration = QUICConfiguration.forClient(
            verificationConfiguration: .rawPublicKeys(publicKeyFilePath: p256PublicKeyPath),
            applicationProtocols: ["h3"]
        )
        configuration.initialPacketSize = .matchingClientInitial(upTo: 1400)
        let coreConfiguration = try StateMachineQUICConnection<NoConsumer>.clientCoreConfiguration(
            configuration,
            serverName: "example.com",
            asyncVerifier: nil,
            remoteAddress: try SocketAddress(ipAddress: ipAddress, port: 4433)
        )

        #expect(coreConfiguration.maxDatagramSize == 1200)
        #expect(coreConfiguration.pmtudEnabled)
        #expect(coreConfiguration.ipUDPHeaderSize == headerSize)
        #expect(coreConfiguration.interfaceMTU == 1400 + headerSize)
    }
}
