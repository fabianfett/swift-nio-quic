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
        var keyLog = Self.configuration
        keyLog.keyLogPath = "/tmp/keys"
        var qlog = Self.configuration
        qlog.qLogConfiguration = QLogConfiguration(path: "/tmp")
        var retry = Self.configuration
        retry.sendRetry = true

        for configuration in [keyLog, qlog, retry] {
            #expect(!QUICConnectionDriver.stateMachineSupports(configuration))
        }
    }

    @Test
    @available(anyAppleOS 26, *)
    func theStateMachineServesTheFeaturesItImplements() {
        var keepalive = Self.configuration
        keepalive.keepAliveInterval = .seconds(5)
        var versionNegotiation = Self.configuration
        versionNegotiation.forceVersionNegotiation = true
        var packetSize = Self.configuration
        packetSize.initialPacketSize = .fixed(1350)
        var noDatagrams = Self.configuration
        noDatagrams.maxDatagramFrameSize = 0

        for configuration in [keepalive, versionNegotiation, packetSize, noDatagrams] {
            #expect(QUICConnectionDriver.stateMachineSupports(configuration))
        }
    }

    /// Every configuration property is either implemented by the state machine driver or checked
    /// by ``QUICConnectionDriver/stateMachineSupports(_:)``: a new one must be added to one of them.
    @Test
    @available(anyAppleOS 26, *)
    func everyConfigurationPropertyIsAllowedOrChecked() {
        let properties = Set(Mirror(reflecting: Self.configuration).children.compactMap(\.label))
        let implemented = QUICConnectionDriver.stateMachineProperties
        let checked = QUICConnectionDriver.stateMachineDefaultOnlyProperties

        #expect(implemented.isDisjoint(with: checked))
        #expect(properties.subtracting(implemented).subtracting(checked).isEmpty)
        #expect(implemented.union(checked).subtracting(properties).isEmpty)
    }
}
