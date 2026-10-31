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
@_spi(ProtocolProvider) import QUICStateMachine
@_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
extension UInt8 {
    /// RFC 9000 § 17.2: The most significant bit (0x80) of byte 0 (the first byte) is set to 1 for long headers.
    fileprivate var indicatesLongHeader: Bool {
        (self & 0x80) != 0
    }

    /// Only keep the bits that identify long header packet types and shift them the right.
    fileprivate var maskedLongHeaderPacketType: UInt8 {
        (self & QUICPacketHeader.PacketType.typeMask) >> 4
    }
}

/// A QUIC packet's header.
@available(anyAppleOS 26, *)
struct QUICPacketHeader: Hashable, Sendable {

    /// QUIC packet header version.
    struct Version: Hashable, Sendable {
        private var backing: UInt32

        static let negotiation = Version(0x0000_0000)
        static let v1 = Version(0x0000_0001)
        static let v2 = Version(0x6b33_43cf)

        var headerVersionField: UInt32 {
            self.backing
        }

        init(_ headerVersionField: UInt32) {
            self.backing = headerVersionField
        }
    }

    /// QUIC packet type.
    struct PacketType: Hashable, Sendable {
        enum Base: UInt8, Hashable {
            /// Initial packet.
            case initial = 1
            /// Retry packet.
            case retry
            /// Handshake packet.
            case handshake
            /// 0-RTT packet.
            case zeroRTT
            /// 1-RTT short header packet.
            case short
            /// Version negotiation packet.
            case versionNegotiation
        }

        fileprivate static let typeMask: UInt8 = 0x30

        let base: Base

        fileprivate init(_ base: Base) {
            self.base = base
        }

        init?(rawValue: UInt8) {
            guard let base = Base(rawValue: rawValue) else {
                return nil
            }
            self.init(base)
        }

        /// Parse the packet type from the byte of the packet.
        init(firstByte: UInt8, version: Version) {
            // RFC 9000 § 17.2: "The most significant bit (0x80) of byte 0
            // (the first byte) is set to 1 for long headers."
            guard firstByte.indicatesLongHeader else {
                // RFC 9000 § 17.3: "The most significant bit (0x80) of
                // byte 0 is set to 0 for the short header."
                self = .short
                return
            }

            // The first byte contains two bits for header types.
            // Mask them and shift them to the right to switch over them.
            //
            // Long Header Packet {
            //   Header Form (1) = 1,
            //   Fixed Bit (1) = 1,
            //   Long Packet Type (2), // <-- These two
            //   ...
            // }
            //
            // The meaning of these bits is QUIC version dependent.
            // QUIC v1 (RFC 9000) and QUIC v2 (RFC 9369) use the same
            // header types, but the bit patterns that indicate them
            // differ. As a result the version is important to identify
            // the packet type.
            //
            // A version negotation packet (RFC 9000 § 17.2.1) carries
            // a version of "0x00000000".
            let maskedByte = firstByte.maskedLongHeaderPacketType

            switch version {
            case Version.negotiation:
                self = .versionNegotiation

            case Version.v1:
                switch maskedByte {
                case 0b00:
                    self = .initial
                case 0b01:
                    self = .zeroRTT
                case 0b10:
                    self = .handshake
                case 0b11:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(maskedByte)")
                }

            case Version.v2:
                switch maskedByte {
                case 0b01:
                    self = .initial
                case 0b10:
                    self = .zeroRTT
                case 0b11:
                    self = .handshake
                case 0b00:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(maskedByte)")
                }

            default:
                // Other versions are unsupported, and a server answers them with Version
                // Negotiation (see `QUICHandler`). This includes:
                // * Forced version negotiation (RFC 9000, Section 15): "Versions that follow the pattern
                //   0x?a?a?a?a are reserved for use in forcing version negotiation to be exercised -- that
                //   is, any version number where the low four bits of all bytes is 1010 (in binary)."
                // * Unrecognized version numbers (RFC 9000, Section 5.2.2): "If a server receives a packet
                //   that indicates an unsupported version and if the packet is large enough to initiate a
                //   new connection for any supported version, the server Version Negotiation packet as
                //   described in Section 6.1."
                self = .versionNegotiation
            }

        }

        /// Initial packet.
        static let initial = PacketType(.initial)
        /// Retry packet.
        static let retry = PacketType(.retry)
        /// Handshake packet.
        static let handshake = PacketType(.handshake)
        /// 0-RTT packet.
        static let zeroRTT = PacketType(.zeroRTT)
        /// 1-RTT short header packet.
        static let short = PacketType(.short)
        /// Version negotiation packet.
        static let versionNegotiation = PacketType(.versionNegotiation)

        var rawValue: UInt8 {
            self.base.rawValue
        }
    }

    /// The type of the packet.
    var type: PacketType
    /// The version of the packet.
    var version: Version?
    /// The destination connection ID of the packet.
    var destinationConnectionID: QUICConnectionID
    /// The source connection ID of the packet.
    var sourceConnectionID: QUICConnectionID?
    /// The address verification token of the packet (RFC 9000 Section 8.1). Only present when
    /// the type is `initial` or `retry`.
    var token: [UInt8]
}

@available(anyAppleOS 26, *)
extension NIOCore.ByteBuffer {

    /// A method to parse a `QUICPacketHeader` from the `ByteBuffer`, mainly to parse the destinationConnectionID only for routing.
    ///
    /// - Parameters:
    ///   - shortHeaderDCIDLength: The length of the destination connection ID. Required to parse short header packets.
    /// - Returns: The parsed `QUICPacketHeader` or `nil`.
    func parseQUICPacketHeader(
        destinationIDLength shortHeaderDCIDLength: Int
    ) -> QUICPacketHeader? {
        self.withUnsafeReadableBytes { buffer in
            // Needed to decide if this is a long or short header packet.
            guard let firstByte = buffer.first else {
                return nil
            }

            let header = QUICConnectionUtilities.parseInboundPacket(
                buffer,
                shortHeaderDestinationCIDLength: Int(shortHeaderDCIDLength)
            )

            guard let header, let dcid = header.destinationConnectionID else {
                return nil
            }

            // NOTE: This will swollow unknown versions. Since the header is only parsed
            // for connection routing (accept new, forward to existing, stateless reset),
            // the accuracy here is not important.
            var version: QUICPacketHeader.Version = .v1
            if let rawVersion = header.version {
                version = QUICPacketHeader.Version(rawVersion)
            }

            let packetType: QUICPacketHeader.PacketType = QUICPacketHeader.PacketType(
                firstByte: firstByte,
                version: version
            )

            var scid: QUICConnectionID? = nil
            if let parsedSCID = header.sourceConnectionID {
                scid = QUICConnectionID(parsedSCID)
            }

            var token = header.token
            if packetType == .initial {
                // The routing parser reads the tokens of Retry packets only. A server validates
                // a client's address with the token of its Initial (RFC 9000 Section 8.1.2).
                guard
                    let initialToken = Self.initialToken(
                        in: buffer,
                        connectionIDsLength: dcid.length + (scid?.length ?? 0)
                    )
                else {
                    return nil
                }
                token = initialToken
            }

            return QUICPacketHeader(
                type: packetType,
                version: version,
                destinationConnectionID: QUICConnectionID(dcid),
                sourceConnectionID: scid,
                token: token
            )
        }
    }

    /// The token of the Initial packet in `packet` (RFC 9000 Section 17.2.2), which follows the
    /// connection IDs and the token's length, or `nil` if the packet ends before it does.
    private static func initialToken(in packet: UnsafeRawBufferPointer, connectionIDsLength: Int) -> [UInt8]? {
        // The first byte, the version, and the lengths of the two connection IDs.
        var index = 1 + 4 + 1 + 1 + connectionIDsLength
        guard index < packet.count else { return nil }
        // A variable-length integer: the two high bits of its first byte give its size (RFC 9000
        // Section 16).
        let lengthSize = 1 << Int(packet[index] >> 6)
        guard lengthSize <= packet.count - index else { return nil }
        var length = UInt64(packet[index] & 0x3f)
        for offset in 1..<lengthSize {
            length = length << 8 | UInt64(packet[index + offset])
        }
        index += lengthSize
        guard length <= UInt64(packet.count - index) else { return nil }
        return Array(packet[index..<(index + Int(length))])
    }
}

@available(anyAppleOS 26, *)
extension QUICPacketHeader {
    /// The shortest datagram the QUIC endpoints of swift-network-evolution read: they drop
    /// shorter ones without parsing them.
    private static var minimumDatagramSize: Int { 21 }

    /// The Version Negotiation packet (RFC 9000 Section 17.2.1) that answers this packet. It lists
    /// QUIC version 1 and a reserved version (RFC 9000 Section 15), and swaps the packet's
    /// connection IDs, as RFC 9000 Section 6.1 requires.
    ///
    /// - Parameter allocator: The allocator for the returned buffer.
    /// - Returns: The datagram, or `nil` if the packet has no source connection ID, or the answer
    ///   would be shorter than the 21 bytes a peer reads.
    func versionNegotiationPacket(allocator: ByteBufferAllocator) -> ByteBuffer? {
        guard let sourceConnectionID = self.sourceConnectionID else {
            return nil
        }
        // The answer goes to the packet's source connection ID, from its destination one.
        let bytes = QUICStatelessPackets.versionNegotiation(
            destinationConnectionID: QUICStateMachine.QUICConnectionID(sourceConnectionID),
            sourceConnectionID: QUICStateMachine.QUICConnectionID(self.destinationConnectionID)
        )
        guard bytes.count >= Self.minimumDatagramSize else {
            return nil
        }

        var buffer = allocator.buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        return buffer
    }

    /// The Retry packet (RFC 9000 Section 17.2.5) that answers this Initial: it asks the client to
    /// send its Initial again, to `sourceConnectionID` and with `token`. The Retry Integrity Tag
    /// covers the packet's destination connection ID (RFC 9001 Section 5.8).
    ///
    /// - Parameters:
    ///   - sourceConnectionID: The connection ID the server chose for the client's next Initial.
    ///   - token: The token the client sends with its next Initial.
    ///   - allocator: The allocator for the returned buffer.
    /// - Returns: The datagram, or `nil` if the packet has no source connection ID, or the tag
    ///   couldn't be computed.
    func retryPacket(
        sourceConnectionID: QUICConnectionID,
        token: [UInt8],
        allocator: ByteBufferAllocator
    ) -> ByteBuffer? {
        guard let clientConnectionID = self.sourceConnectionID else {
            return nil
        }
        // Only version 1 Initials start connections, see `QUICHandler`.
        let bytes = QUICStatelessPackets.retry(
            version: .v1,
            destinationConnectionID: QUICStateMachine.QUICConnectionID(clientConnectionID),
            sourceConnectionID: QUICStateMachine.QUICConnectionID(sourceConnectionID),
            originalDestinationConnectionID: QUICStateMachine.QUICConnectionID(self.destinationConnectionID),
            token: token
        )
        guard let bytes else {
            return nil
        }

        var buffer = allocator.buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        return buffer
    }
}
