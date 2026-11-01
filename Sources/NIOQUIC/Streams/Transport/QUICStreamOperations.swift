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

import NIOQUICHelpers

/// What a stream table's streams do on the connection that carries them: open, read, write,
/// finish, reset and stop.
///
/// A ``StateMachineQUICConnection`` does them on its `QUICConnectionCore`. Tests do them without a
/// connection.
@available(anyAppleOS 26, *)
protocol QUICStreamOperations: AnyObject {
    /// Opens a stream for the slot `handle`, or queues it until the peer allows more streams. The
    /// stream table learns its ID through ``QUICStreamTable/assignID(_:to:)``, and reports it as an
    /// ``QUICStreamEvents/opened`` visit.
    func openStream(for handle: QUICStreamHandle, bidirectional: Bool)

    /// Forgets a stream ``openStream(for:bidirectional:)`` queued, which never got an ID.
    func cancelPendingOpen(_ handle: QUICStreamHandle)

    /// Calls `body` with the readable bytes of a stream, and whether they run to its end; `body`
    /// returns how many bytes it consumed.
    ///
    /// - Returns: What `body` returned, or `nil` if there is no such stream.
    func read<Result>(
        _ streamID: QUICStreamID,
        _ body: (RawSpan, _ isFinished: Bool) -> (consumed: Int, result: Result)
    ) -> Result?

    /// The bytes a stream takes before it reaches the peer's flow control limit.
    func writableBytes(_ streamID: QUICStreamID) -> Int

    /// Queues bytes on a stream, and finishes it with `fin`.
    ///
    /// - Returns: How many bytes were taken: all of them, or none if the stream or the connection
    ///   can't send any more.
    @discardableResult
    func write(_ bytes: RawSpan, to streamID: QUICStreamID, fin: Bool) -> Int

    /// Finishes the send side of a stream.
    func finish(_ streamID: QUICStreamID)

    /// Resets the send side of a stream with RESET\_STREAM.
    func resetStream(_ streamID: QUICStreamID, code: QUICApplicationErrorCode)

    /// Asks the peer to stop sending on a stream with STOP\_SENDING.
    func stopSending(_ streamID: QUICStreamID, code: QUICApplicationErrorCode)
}
