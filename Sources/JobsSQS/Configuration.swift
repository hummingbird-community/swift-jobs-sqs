//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

extension SQSJobQueue {
    public struct Configuration: Sendable {
        public var queueName: String
        public var visibilityTimeoutInSeconds: Int
        public var pollTime: Duration

        public init(queueName: String, visibilityTimeoutInSeconds: Int = 5 * 60, pollTime: Duration = .milliseconds(100)) {
            self.queueName = queueName
            self.visibilityTimeoutInSeconds = visibilityTimeoutInSeconds
            self.pollTime = pollTime
        }
    }
}
