//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-jobs-sqs project
//
// Copyright (c) 2025 the swift-jobs-sqs authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import SotoCore

extension SQSJobQueue {
    public struct Configuration: Sendable {
        public var queueName: String
        public var region: Region
        public var endpoint: String?
        public var visibilityTimeoutInSeconds: Int
        public var pollTime: Duration

        public init(
            queueName: String,
            region: Region,
            endpoint: String? = nil,
            visibilityTimeoutInSeconds: Int = 5 * 60,
            pollTime: Duration = .milliseconds(100)
        ) {
            self.queueName = queueName
            self.region = region
            self.endpoint = endpoint
            self.visibilityTimeoutInSeconds = visibilityTimeoutInSeconds
            self.pollTime = pollTime
        }
    }
}
