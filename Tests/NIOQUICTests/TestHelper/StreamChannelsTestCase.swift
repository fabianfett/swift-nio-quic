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

import Synchronization
import XCTest

@testable import NIOQUIC

/// A test case whose tests run once on each driver: their handlers serve the stream channels on
/// ``streamChannelsDriver``, the SwiftNetwork driver, then the state machine driver. Failures name
/// the driver. swift-corelibs-xctest can't run a test twice, so on Linux they run on the driver
/// `NIOQUIC_DRIVER` selects, the state machine's by default.
///
/// Every stream channel connection a run makes must be on its driver, and the tests of a class
/// must make at least one on each, so that a silent fallback can't pass. XCTest runs one test at
/// a time, which the driver override this sets for the whole process relies on.
@available(anyAppleOS 26, *)
class StreamChannelsTestCase: XCTestCase {
    /// The drivers each test runs on, in order.
    #if canImport(Darwin)
    static let streamChannelsDrivers: [QUICConnectionDriver] = [.swiftNetwork, .stateMachine]
    #else
    static let streamChannelsDrivers: [QUICConnectionDriver] = [QUICConnectionDriver.default(for: .server)]
    #endif

    /// The driver the handlers of the current run serve their stream channels on.
    private(set) var streamChannelsDriver: QUICConnectionDriver = StreamChannelsTestCase.streamChannelsDrivers[0]

    /// Set by a test whose connections fall back to the SwiftNetwork driver, as those with
    /// features only it implements do: at least one must.
    var fallsBackToSwiftNetwork = false

    /// How many stream channel connections the tests of each class made, by driver.
    private static let connectionsOnDriver = Mutex<[ObjectIdentifier: [QUICConnectionDriver: Int]]>([:])

    #if canImport(Darwin)
    override func invokeTest() {
        for driver in Self.streamChannelsDrivers {
            self.streamChannelsDriver = driver
            self.fallsBackToSwiftNetwork = false
            super.invokeTest()
        }
    }
    #endif

    override func setUp() {
        super.setUp()
        QUICConnectionDriver._forTesting_streamChannelsConnections.withLock { $0.removeAll() }
        QUICConnectionDriver._forTesting_streamChannelsDriver.withLock { $0 = self.streamChannelsDriver }
    }

    override func tearDown() {
        QUICConnectionDriver._forTesting_streamChannelsDriver.withLock { $0 = nil }
        let drivers = QUICConnectionDriver._forTesting_streamChannelsConnections.withLock { $0 }
        if self.fallsBackToSwiftNetwork {
            XCTAssert(drivers.contains(.swiftNetwork), "no stream channel connection fell back: \(drivers)")
        } else {
            XCTAssert(
                drivers.allSatisfy { $0 == self.streamChannelsDriver },
                "stream channel connections ran on \(drivers), not only on \(self.streamChannelsDriver)"
            )
        }
        Self.connectionsOnDriver.withLock {
            $0[ObjectIdentifier(Self.self), default: [:]][self.streamChannelsDriver, default: 0] += drivers.count
        }
        super.tearDown()
    }

    override class func tearDown() {
        let connections = Self.connectionsOnDriver.withLock { $0[ObjectIdentifier(Self.self)] ?? [:] }
        for driver in Self.streamChannelsDrivers {
            XCTAssertGreaterThan(
                connections[driver] ?? 0,
                0,
                "\(Self.self) made no stream channel connection on \(driver)"
            )
        }
        super.tearDown()
    }

    #if canImport(Darwin)
    override func record(_ issue: XCTIssue) {
        var issue = issue
        issue.compactDescription = "[\(self.streamChannelsDriver)] \(issue.compactDescription)"
        super.record(issue)
    }
    #else
    override func recordFailure(
        withDescription description: String,
        inFile filePath: String,
        atLine lineNumber: Int,
        expected: Bool
    ) {
        super.recordFailure(
            withDescription: "[\(self.streamChannelsDriver)] \(description)",
            inFile: filePath,
            atLine: lineNumber,
            expected: expected
        )
    }
    #endif
}
