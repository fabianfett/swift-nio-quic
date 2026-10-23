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

import Testing

@testable import NIOQUIC

@Suite
struct QUICConnectionDriverTests {
    @available(anyAppleOS 26, *)
    private static var configuration: QUICConfiguration {
        QUICConfiguration.forClient(
            verificationConfiguration: .rawPublicKeys(publicKeyFilePath: "unused"),
            applicationProtocols: ["test"]
        )
    }

    @Test
    @available(anyAppleOS 26, *)
    func theStateMachineServesADefaultConfiguration() {
        #expect(QUICConnectionDriver.stateMachineSupports(Self.configuration))
    }

    @Test
    @available(anyAppleOS 26, *)
    func featuresTheStateMachineLacksFallBackToSwiftNetwork() {
        var keepalive = Self.configuration
        keepalive.keepAliveInterval = .seconds(5)
        var keyLog = Self.configuration
        keyLog.keyLogPath = "/tmp/keys"
        var qlog = Self.configuration
        qlog.qLogConfiguration = QLogConfiguration(path: "/tmp")
        var retry = Self.configuration
        retry.sendRetry = true
        var versionNegotiation = Self.configuration
        versionNegotiation.forceVersionNegotiation = true

        for configuration in [keepalive, keyLog, qlog, retry, versionNegotiation] {
            #expect(!QUICConnectionDriver.stateMachineSupports(configuration))
        }
    }
}
