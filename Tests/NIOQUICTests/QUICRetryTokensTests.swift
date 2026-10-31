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
import Testing

@testable import NIOQUIC

struct QUICRetryTokensTests {
    @available(anyAppleOS 26, *)
    private static var originalDestinationConnectionID: QUICConnectionID {
        QUICConnectionID(bytes: InlineArray(repeating: 1), length: 8)
    }

    @available(anyAppleOS 26, *)
    private static var retrySourceConnectionID: QUICConnectionID {
        QUICConnectionID(bytes: InlineArray(repeating: 2), length: 8)
    }

    private static var now: NIODeadline { .uptimeNanoseconds(1_000_000_000_000) }

    @available(anyAppleOS 26, *)
    private static func makeToken(
        _ tokens: QUICRetryTokens = QUICRetryTokens(key: [UInt8](repeating: 7, count: 32)),
        clientAddress: SocketAddress? = nil,
        now: NIODeadline = Self.now
    ) throws -> [UInt8] {
        tokens.makeToken(
            originalDestinationConnectionID: Self.originalDestinationConnectionID,
            retrySourceConnectionID: Self.retrySourceConnectionID,
            clientAddress: try clientAddress ?? SocketAddress(ipAddress: "192.0.2.1", port: 4433),
            now: now
        )
    }

    @available(anyAppleOS 26, *)
    private static func validate(
        _ token: [UInt8],
        _ tokens: QUICRetryTokens = QUICRetryTokens(key: [UInt8](repeating: 7, count: 32)),
        retrySourceConnectionID: QUICConnectionID = Self.retrySourceConnectionID,
        clientAddress: SocketAddress? = nil,
        now: NIODeadline = Self.now
    ) throws -> QUICRetryTokens.Validation {
        tokens.validate(
            token,
            retrySourceConnectionID: retrySourceConnectionID,
            clientAddress: try clientAddress ?? SocketAddress(ipAddress: "192.0.2.1", port: 4433),
            now: now
        )
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aTokenCarriesTheOriginalDestinationConnectionID() throws {
        let token = try Self.makeToken()

        #expect(
            try Self.validate(token) == .valid(originalDestinationConnectionID: Self.originalDestinationConnectionID)
        )
        // A client's port may change behind a NAT: only its IP address counts.
        #expect(
            try Self.validate(token, clientAddress: SocketAddress(ipAddress: "192.0.2.1", port: 5000))
                == .valid(originalDestinationConnectionID: Self.originalDestinationConnectionID)
        )
        // Tokens fit the 128 bytes a client follows a Retry with.
        #expect(token.count <= 128)
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aTokenIsValidForTenSeconds() throws {
        let token = try Self.makeToken()

        #expect(
            try Self.validate(token, now: Self.now + .seconds(10))
                == .valid(originalDestinationConnectionID: Self.originalDestinationConnectionID)
        )
        #expect(try Self.validate(token, now: Self.now + .seconds(10) + .nanoseconds(1)) == .invalid)
        // Not made yet.
        #expect(try Self.validate(token, now: Self.now - .nanoseconds(1)) == .invalid)
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aChangedTokenIsInvalid() throws {
        let token = try Self.makeToken()

        // The time, the connection ID, and the tag: every byte is authenticated. The tenth byte,
        // the connection ID's length, also sets the token's length: see `otherTokensAreUnknown`.
        for index in 1..<token.count where index != 9 {
            var forged = token
            forged[index] ^= 0x01
            #expect(try Self.validate(forged) == .invalid, "byte \(index)")
        }
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func aTokenIsOnlyValidForItsAddressRetryAndKey() throws {
        let token = try Self.makeToken()

        #expect(try Self.validate(token, clientAddress: SocketAddress(ipAddress: "192.0.2.2", port: 4433)) == .invalid)
        #expect(try Self.validate(token, clientAddress: SocketAddress(ipAddress: "::1", port: 4433)) == .invalid)
        #expect(
            try Self.validate(token, retrySourceConnectionID: QUICConnectionID(bytes: InlineArray(repeating: 3), length: 8))
                == .invalid
        )
        #expect(try Self.validate(token, QUICRetryTokens()) == .invalid)
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func tokensWorkForIPv6Clients() throws {
        let address = try SocketAddress(ipAddress: "2001:db8::1", port: 4433)
        let token = try Self.makeToken(clientAddress: address)

        #expect(
            try Self.validate(token, clientAddress: address)
                == .valid(originalDestinationConnectionID: Self.originalDestinationConnectionID)
        )
        #expect(try Self.validate(token, clientAddress: SocketAddress(ipAddress: "2001:db8::2", port: 4433)) == .invalid)
    }

    @Test(.timeLimit(.minutes(1)))
    @available(anyAppleOS 26, *)
    func otherTokensAreUnknown() throws {
        let token = try Self.makeToken()

        // Tokens of other kinds, which a server checks less strictly (RFC 9000 Section 8.1.3).
        #expect(try Self.validate([]) == .unknown)
        #expect(try Self.validate([1, 2, 3, 4]) == .unknown)
        var otherMarker = token
        otherMarker[0] ^= 0x01
        #expect(try Self.validate(otherMarker) == .unknown)
        #expect(try Self.validate(Array(token.dropLast())) == .unknown)
        #expect(try Self.validate(token + [0]) == .unknown)
        var otherLength = token
        otherLength[9] ^= 0x01
        #expect(try Self.validate(otherLength) == .unknown)
    }
}
