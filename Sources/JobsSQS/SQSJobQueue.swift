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

import Jobs
import Logging
import NIOCore
import SotoCore
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public final class SQSJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible, Equatable, Hashable {
        let value: String

        init() {
            self.value = UUID().uuidString
        }

        init(value: String) {
            self.value = value
        }

        /// String description of Identifier
        public var description: String {
            self.value
        }
    }
    /// Options for job pushed to queue
    public struct JobOptions: JobOptionsProtocol {
        /// Delay running job until
        public var delayUntil: Date?

        /// Default initializer for JobOptions
        public init() {
            self.delayUntil = nil
        }

        ///  Initializer for JobOptions
        /// - Parameter delayUntil: Whether job execution should be delayed until a later date
        public init(delayUntil: Date) {
            self.delayUntil = delayUntil
        }
    }
    public struct SQSQueueError: Error, CustomStringConvertible {
        public enum Code: Sendable {
            case unexpectedSQSResponse
            case internalError
        }
        let code: Code
        let message: String?

        init(_ code: Code, message: String? = nil) {
            self.code = code
            self.message = message
        }

        public var description: String {
            switch self.code {
            case .unexpectedSQSResponse:
                return "Unexpected response from SQS \(message ?? "")"
            case .internalError:
                return "Internal Error: \(message ?? "")"
            }
        }
    }
    struct ActiveJob {
        let body: String
        let receiptHandle: String
    }
    /// what to do with failed jobs from last time queue was handled
    public enum JobCleanup: Sendable {
        case doNothing
        case rerun
        case remove
    }

    let configuration: Configuration
    let sqs: SQS
    let ssm: SSM
    let queueURL: String
    let failedQueueURL: String
    let jobRegistry: JobRegistry
    let activeJobs: Mutex<[JobID: ActiveJob]>
    let isStopped: Atomic<Bool>

    public init(awsClient: AWSClient, configuration: Configuration, logger: Logger) async throws {
        self.sqs = SQS(client: awsClient, region: configuration.region, endpoint: configuration.endpoint)
        self.ssm = SSM(client: awsClient, region: configuration.region, endpoint: configuration.endpoint)
        self.configuration = configuration
        self.jobRegistry = .init()
        self.queueURL = try await Self.createQueue(queueName: "\(configuration.queueName)", sqs: sqs, logger: logger)
        self.failedQueueURL = try await Self.createQueue(queueName: "\(configuration.queueName)_failed", sqs: sqs, logger: logger)
        self.activeJobs = .init([:])
        self.isStopped = .init(false)
    }

    static func createQueue(queueName: String, sqs: SQS, logger: Logger) async throws -> String {
        do {
            let getQueueURLResponse = try await sqs.getQueueUrl(queueName: queueName, logger: logger)
            guard let queueUrl = getQueueURLResponse.queueUrl else {
                throw SQSQueueError(.unexpectedSQSResponse, message: "GetQueueURL did not return a URL.")
            }
            return queueUrl
        } catch let error as SQSErrorType where error == .queueDoesNotExist {
            // queue does not exist so lets create it
            let createQueueResponse = try await sqs.createQueue(
                queueName: queueName,
                logger: logger
            )
            guard let queueUrl = createQueueResponse.queueUrl else {
                throw SQSQueueError(.unexpectedSQSResponse, message: "CreateQueue did not return a URL.")
            }
            logger.info("Created Queue", metadata: ["queueName": .string(queueName), "queueURL": .string(queueUrl)])
            try await Task.sleep(for: .milliseconds(500))
            return queueUrl
        }
    }

    ///  Cleanup job queues
    ///
    /// This function is used to re-run or delete jobs on the failed queue. Failed jobs can be
    /// pushed back into the pending queue to be re-run or removed.
    ///
    /// The job queue needs to be running when you call cleanup.
    ///
    /// - Parameters:
    ///   - failedJobs: What to do with jobs in the failed queue
    /// - Throws:
    public func cleanup(
        failedJobs: JobCleanup = .doNothing
    ) async throws {
        while true {
            let receiveMessageResponse = try await self.sqs.receiveMessage(
                maxNumberOfMessages: 10,
                messageAttributeNames: ["id"],
                queueUrl: self.failedQueueURL,
                visibilityTimeout: configuration.visibilityTimeoutInSeconds
            )
            guard let messages = receiveMessageResponse.messages, messages.count > 0 else { break }
            for message in messages {
                guard let receiptHandle = message.receiptHandle else {
                    throw SQSQueueError(.unexpectedSQSResponse, message: "Failed message doesnt have a receipt handle")
                }
                guard let body = message.body else {
                    throw SQSQueueError(.unexpectedSQSResponse, message: "Failed message doesnt have a body")
                }
                guard let jobIDAttribute = message.messageAttributes?["id"] else { continue }
                switch failedJobs {
                case .rerun:
                    _ = try await self.sqs.sendMessage(
                        messageAttributes: ["id": jobIDAttribute],
                        messageBody: body,
                        queueUrl: self.queueURL
                    )
                    try await self.sqs.deleteMessage(queueUrl: self.failedQueueURL, receiptHandle: receiptHandle)
                case .remove:
                    try await self.sqs.deleteMessage(queueUrl: self.failedQueueURL, receiptHandle: receiptHandle)
                case .doNothing:
                    break
                }
            }
        }
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters: Codable & Sendable>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - buffer: Encoded Job data
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let jobInstanceID = JobID()
        try await self.push(jobID: jobInstanceID, jobRequest: jobRequest, options: options)
        return jobInstanceID
    }

    /// Retry job data onto queue
    /// - Parameters:
    ///   - id: Job instance ID
    ///   - jobRequest: Job request
    ///   - options: Job retry options
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let options = JobOptions(delayUntil: options.delayUntil)
        //try await self.finished(jobID: id)
        try await self.push(jobID: id, jobRequest: jobRequest, options: options)
    }

    /// Helper for enqueuing jobs
    private func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        let delaySeconds = options.delayUntil.map { Swift.max(Int($0.timeIntervalSinceNow.rounded(.up)), 0) }
        _ = try await self.sqs.sendMessage(
            delaySeconds: delaySeconds,
            messageAttributes: ["id": SQS.MessageAttributeValue(dataType: "String", stringValue: jobID.description)],
            messageBody: String(buffer: buffer),
            queueUrl: self.queueURL
        )
    }

    /// Flag job is done
    ///
    /// - Parameters:
    ///   - jobID: Job id
    public func finished(jobID: JobID) async throws {
        guard let job = self.activeJobs.withLock({ $0[jobID] }) else {
            throw SQSQueueError(.internalError, message: "Queue receipt is not available for finished job")
        }
        try await self.sqs.deleteMessage(queueUrl: self.queueURL, receiptHandle: job.receiptHandle)
    }

    /// Flag job failed to process
    ///
    /// - Parameters:
    ///   - jobID: Job id
    public func failed(jobID: JobID, error: Error) async throws {
        guard let job = self.activeJobs.withLock({ $0[jobID] }) else {
            throw SQSQueueError(.internalError, message: "Queue receipt is not available for finished job")
        }
        // delete from pending queue ...
        try await self.sqs.deleteMessage(queueUrl: self.queueURL, receiptHandle: job.receiptHandle)
        // and add to failed queue
        _ = try await self.sqs.sendMessage(
            messageAttributes: ["id": SQS.MessageAttributeValue(dataType: "String", stringValue: jobID.description)],
            messageBody: job.body,
            queueUrl: self.failedQueueURL
        )
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Get job queue metadata
    /// - Parameter key: Metadata key
    /// - Returns: Associated ByteBuffer
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        let response = try await ssm.getParameter(name: "/swift-jobs/\(key)")
        return response.parameter?.value.map { ByteBuffer(string: $0) }
    }

    /// Set job queue metadata
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Associated ByteBuffer
    public func setMetadata(key: String, value: ByteBuffer) async throws {
        _ = try await ssm.putParameter(name: "/swift-jobs/\(key)", overwrite: true, type: .string, value: String(buffer: value))
    }

    func popFirst() async throws -> JobQueueResult<JobID>? {
        let receiveMessageResponse = try await self.sqs.receiveMessage(
            maxNumberOfMessages: 1,
            messageAttributeNames: ["id"],
            queueUrl: self.queueURL,
            visibilityTimeout: configuration.visibilityTimeoutInSeconds
        )
        guard let message = receiveMessageResponse.messages?.first else { return nil }
        guard let jobIDAttribute = message.messageAttributes?["id"] else {
            throw SQSQueueError(.unexpectedSQSResponse, message: "Message doesnt have attribute 'id'")
        }
        guard let jobIDString = jobIDAttribute.stringValue else {
            throw SQSQueueError(.unexpectedSQSResponse, message: "Message attribute 'id' is not a string value")
        }
        guard let receiptHandle = message.receiptHandle else {
            throw SQSQueueError(.unexpectedSQSResponse, message: "Message doesnt have a receipt handle")
        }
        guard let body = message.body else {
            throw SQSQueueError(.unexpectedSQSResponse, message: "Message doesnt have a body")
        }

        let jobID = JobID(value: jobIDString)
        let activeJob = ActiveJob(body: body, receiptHandle: receiptHandle)
        self.activeJobs.withLock { $0[jobID] = activeJob }
        do {
            let jobInstance = try self.jobRegistry.decode(ByteBuffer(string: body))
            return .init(id: jobID, result: .success(jobInstance))
        } catch let error as JobQueueError {
            return .init(id: jobID, result: .failure(error))
        }

    }
}

/// extend RedisJobQueue to conform to AsyncSequence
extension SQSJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public struct AsyncIterator: AsyncIteratorProtocol {
        let queue: SQSJobQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.load(ordering: .relaxed) {
                    return nil
                }
                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: self.queue.configuration.pollTime)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self)
    }
}

extension JobQueueDriver where Self == SQSJobQueue {
    /// Return SQS driver for Job Queue
    /// - Parameters:
    ///   - redisConnectionPool: Redis connection pool
    ///   - configuration: configuration
    public static func sqs(awsClient: AWSClient, configuration: SQSJobQueue.Configuration, logger: Logger) async throws -> Self {
        try await .init(awsClient: awsClient, configuration: configuration, logger: logger)
    }
}
