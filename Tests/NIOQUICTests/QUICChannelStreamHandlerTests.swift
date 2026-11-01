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

import Foundation
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@Suite("NetworkError to QUIC error conversion")
struct NetworkErrorConversionTests {
    @available(anyAppleOS 26, *)
    @Test
    func applicationErrorCodeConverts() throws {
        let error = try #require(NetworkError(quicApplicationError: UInt64(261), reason: "no headers"))
        let converted = try #require(QUICConnectionError(networkError: error))
        #expect(converted.isApplication)
        #expect(converted.code == 261)
        #expect(converted.reason == error.description)
    }

    @available(anyAppleOS 26, *)
    @Test
    func transportErrorCodeConverts() throws {
        let error = try #require(NetworkError(quicTransportError: UInt64(10), reason: "protocol violation"))
        let converted = try #require(QUICConnectionError(networkError: error))
        #expect(!converted.isApplication)
        #expect(converted.code == 10)
        #expect(converted.reason == error.description)
    }

    @available(anyAppleOS 26, *)
    @Test
    func errorWithoutQUICCodeDoesNotConvert() {
        // The caller keeps such errors as-is; there is no QUIC-level equivalent to convert to.
        let converted = QUICConnectionError(networkError: .posix(ETIMEDOUT))
        #expect(converted == nil)
    }
}
