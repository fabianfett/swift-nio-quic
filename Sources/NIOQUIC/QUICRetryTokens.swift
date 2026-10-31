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
import NIOCore

/// Makes and checks the tokens of the Retry packets a server sends to validate a client's address
/// before it keeps any state for the client (RFC 9000 Section 8.1.2).
///
/// A token carries the destination connection ID of the client's first Initial, which the server
/// needs for the client's next Initial, and when it was made. An HMAC-SHA256 tag with a key of the
/// token maker authenticates both, the client's IP address, and the source connection ID of the
/// Retry, which the client sends its next Initial to. Only the IP address counts: a NAT may
/// give the client another port meanwhile.
///
/// Tokens start with a byte that sets them apart from tokens of other kinds (RFC 9000 Section
/// 8.1.1): only those are checked.
@available(anyAppleOS 26, *)
struct QUICRetryTokens {
    /// How long a token is valid: a client sends it right after the Retry, so one round trip and
    /// some.
    static var lifetime: TimeAmount { .seconds(10) }

    /// The first byte of a token.
    private static var marker: UInt8 { 0x52 }

    /// The size of a token's tag.
    private static var tagSize: Int { SHA256.byteCount }

    /// The size of a token without its connection ID: the marker, the time it was made, the
    /// connection ID's length, and the tag.
    private static var fixedSize: Int { 1 + 8 + 1 + Self.tagSize }

    /// What checking a client's token found.
    enum Validation: Equatable {
        /// The token is one of ours and valid: the client's first Initial went to
        /// `originalDestinationConnectionID`.
        case valid(originalDestinationConnectionID: QUICConnectionID)
        /// The token is one of ours, but forged, expired, or made for another address or Retry.
        case invalid
        /// The token isn't a Retry token of this kind: the client's address isn't validated.
        case unknown
    }

    private let key: SymmetricKey

    /// - Parameter key: The key of the tags, or `nil` for a random one. Tokens are only valid
    ///   with the key they were made with.
    init(key: [UInt8]? = nil) {
        self.key = key.map { SymmetricKey(data: $0) } ?? SymmetricKey(size: .bits256)
    }

    /// The token of a Retry with the source connection ID `retrySourceConnectionID`, the answer to
    /// a client's Initial from `clientAddress` to `originalDestinationConnectionID`.
    func makeToken(
        originalDestinationConnectionID: QUICConnectionID,
        retrySourceConnectionID: QUICConnectionID,
        clientAddress: SocketAddress,
        now: NIODeadline
    ) -> [UInt8] {
        var token = [UInt8]()
        token.reserveCapacity(Self.fixedSize + originalDestinationConnectionID.length)
        token.append(Self.marker)
        Self.append(now.uptimeNanoseconds, to: &token)
        token.append(UInt8(originalDestinationConnectionID.length))
        originalDestinationConnectionID.withUnsafeBufferPointer { token.append(contentsOf: $0) }
        let tag = HMAC<SHA256>.authenticationCode(
            for: Self.authenticatedBytes(
                of: token,
                retrySourceConnectionID: retrySourceConnectionID,
                clientAddress: clientAddress
            ),
            using: self.key
        )
        token.append(contentsOf: tag)
        return token
    }

    /// Checks the token of a client's Initial from `clientAddress` to `retrySourceConnectionID`.
    func validate(
        _ token: [UInt8],
        retrySourceConnectionID: QUICConnectionID,
        clientAddress: SocketAddress,
        now: NIODeadline
    ) -> Validation {
        guard token.count >= Self.fixedSize, token[0] == Self.marker else { return .unknown }
        let connectionIDLength = Int(token[9])
        guard connectionIDLength <= Int(QUICConnectionID.maxLength),
            token.count == Self.fixedSize + connectionIDLength
        else {
            return .unknown
        }
        let tagStart = token.count - Self.tagSize
        let body = Array(token[..<tagStart])
        // A constant-time comparison: the time to reject a tag tells nothing about it.
        guard
            HMAC<SHA256>.isValidAuthenticationCode(
                token[tagStart...],
                authenticating: Self.authenticatedBytes(
                    of: body,
                    retrySourceConnectionID: retrySourceConnectionID,
                    clientAddress: clientAddress
                ),
                using: self.key
            )
        else {
            return .invalid
        }
        let madeAt = token[1..<9].reduce(UInt64(0)) { $0 << 8 | UInt64($1) }
        let made = NIODeadline.uptimeNanoseconds(madeAt)
        guard made <= now, now - made <= Self.lifetime else { return .invalid }
        let originalDestinationConnectionID = QUICConnectionID(bytes: Array(token[10..<tagStart]).span)
        return .valid(originalDestinationConnectionID: originalDestinationConnectionID)
    }

    /// The bytes a token's tag authenticates: the token before its tag, the Retry's source
    /// connection ID, and the client's IP address.
    private static func authenticatedBytes(
        of body: [UInt8],
        retrySourceConnectionID: QUICConnectionID,
        clientAddress: SocketAddress
    ) -> [UInt8] {
        var bytes = body
        bytes.append(UInt8(retrySourceConnectionID.length))
        retrySourceConnectionID.withUnsafeBufferPointer { bytes.append(contentsOf: $0) }
        switch clientAddress {
        case .v4(let address):
            bytes.append(4)
            withUnsafeBytes(of: address.address.sin_addr) { bytes.append(contentsOf: $0) }
        case .v6(let address):
            bytes.append(6)
            withUnsafeBytes(of: address.address.sin6_addr) { bytes.append(contentsOf: $0) }
        case .unixDomainSocket:
            preconditionFailure("QUIC runs over UDP")
        }
        return bytes
    }

    private static func append(_ value: UInt64, to bytes: inout [UInt8]) {
        for shift in stride(from: 56, through: 0, by: -8) {
            bytes.append(UInt8(truncatingIfNeeded: value >> UInt64(shift)))
        }
    }
}
