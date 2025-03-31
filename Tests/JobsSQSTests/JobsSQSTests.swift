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

import Foundation
import Jobs
import ServiceLifecycle
import SotoCore
import Synchronization
import Testing

@testable import JobsSQS

final class SQSJobsTests {
    let awsClient = AWSClient(  //middleware: AWSLoggingMiddleware()
        )

    deinit {
        try? awsClient.syncShutdown()
    }

    var sqsEndpoint: String? {
        ProcessInfo.processInfo.environment["LOCALSTACK_ENDPOINT"]
    }

    public func withSQSQueue<Value>(
        queueName: String,
        sqs: SQS,
        delete: Bool = true,
        _ operation: () async throws -> Value
    ) async throws -> Value {
        let value: Value
        do {
            value = try await operation()
        } catch {
            guard let queueURL = try? await sqs.getQueueUrl(queueName: "\(queueName)").queueUrl else { throw error }
            try? await sqs.deleteQueue(queueUrl: queueURL)
            guard let failedQueueURL = try? await sqs.getQueueUrl(queueName: "\(queueName)_failed").queueUrl else { throw error }
            try? await sqs.deleteQueue(queueUrl: failedQueueURL)
            throw error
        }
        if delete {
            guard let queueURL = try await sqs.getQueueUrl(queueName: "\(queueName)").queueUrl else { return value }
            try await sqs.deleteQueue(queueUrl: queueURL)
            guard let failedQueueURL = try await sqs.getQueueUrl(queueName: "\(queueName)_failed").queueUrl else { return value }
            try? await sqs.deleteQueue(queueUrl: failedQueueURL)
        }
        return value
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function and ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        failedJobsInitialization: SQSJobQueue.JobCleanup = .remove,
        queuePrefix: String = #function,
        queueName: String? = nil,
        cleanUpQueue: Bool = true,
        test: (JobQueue<SQSJobQueue>) async throws -> T
    ) async throws -> T {
        let queueName = queueName ?? queuePrefix.filter(\.isLetter) + UUID().uuidString
        var logger = Logger(label: queuePrefix.filter(\.isLetter))
        logger.logLevel = .debug
        let sqs = SQS(
            client: self.awsClient,
            region: .euwest1,
            endpoint: sqsEndpoint
        )
        return try await withSQSQueue(queueName: queueName, sqs: sqs, delete: cleanUpQueue) {
            let jobQueue = try await JobQueue(
                .sqs(
                    awsClient: self.awsClient,
                    configuration: .init(queueName: queueName, region: .euwest1, endpoint: self.sqsEndpoint),
                    logger: logger
                ),
                numWorkers: numWorkers,
                logger: logger,
                options: .init(
                    defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10))
                )
            )

            return try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [jobQueue],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: Logger(label: "JobQueueService")
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                try await jobQueue.queue.cleanup(failedJobs: failedJobsInitialization)
                let value = try await test(jobQueue)
                await serviceGroup.triggerGracefulShutdown()
                return value
            }
        }
    }

    @Test
    func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let counter = Counter()
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                counter.trigger()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            try await counter.waitFor(count: 10)
        }
    }
    @Test
    func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = Atomic(0)
        let maxRunningJobCounter = Atomic(0)
        let counter = Counter()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                let runningJobs = runningJobCounter.wrappingAdd(1, ordering: .relaxed)
                if runningJobs.newValue > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs.newValue, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                counter.trigger()
                runningJobCounter.wrappingSubtract(1, ordering: .relaxed)
            }

            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            try await counter.waitFor(count: 10)

            #expect(maxRunningJobCounter.load(ordering: .relaxed) >= 1)
            #expect(maxRunningJobCounter.load(ordering: .relaxed) <= 4)
        }
    }

    @Test
    func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        struct FailedError: Error {}
        let counter = Counter()
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                counter.trigger()
                throw FailedError()
            }
            try await jobQueue.push(TestParameters())

            try await counter.waitFor(count: 4)

            try await Task.sleep(for: .milliseconds(200))
        }
    }

    @Test
    func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let counter = Counter()
        let currentJobTryCount: Mutex<Int> = .init(0)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                defer {
                    currentJobTryCount.withLock {
                        $0 += 1
                    }
                }
                counter.trigger()
                if currentJobTryCount.withLock({ $0 }) == 0 {
                    throw FailedError()
                }
            }
            try await jobQueue.push(TestParameters())

            try await counter.waitFor(count: 2)

            try await Task.sleep(for: .milliseconds(200))
        }
        #expect(currentJobTryCount.withLock { $0 } == 2)
    }

    @Test
    func testJobSerialization() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName = "testJobSerialization"
            let id: Int
            let message: String
        }
        let counter = Counter()
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
                #expect(parameters.id == 23)
                #expect(parameters.message == "Hello!")
                counter.trigger()
            }
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            try await counter.waitFor(count: 1)
        }
    }

    @Test
    func testDelayedJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDelayedJob"
            let value: Int
        }
        let counter = Counter()
        let jobExecutionSequence: Mutex<[Int]> = .init([])
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, _ in
                jobExecutionSequence.withLock {
                    $0.append(parameters.value)
                }
                counter.trigger()
            }
            try await jobQueue.push(
                TestParameters(value: 100),
                options: .init(delayUntil: Date.now.addingTimeInterval(2))
            )
            try await jobQueue.push(TestParameters(value: 50))
            try await jobQueue.push(TestParameters(value: 10))

            try await counter.waitFor(count: 3)
        }
        jobExecutionSequence.withLock {
            #expect($0 == [50, 10, 100] || $0 == [10, 50, 100])
        }
    }

    /// Test job is cancelled on shutdown
    @Test
    func testShutdownJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testShutdownJob"
        }
        let counter = Counter()
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { _, _ in
                counter.trigger()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(TestParameters())
            try await counter.waitFor(count: 1)
        }
    }

    /// test job fails to decode but queue continues to process
    @Test
    func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: Mutex<String> = .init("")
        let counter = Counter()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
                string.withLock { $0 = parameters.value }
                counter.trigger()
            }
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            try await counter.waitFor(count: 1)
        }
        string.withLock {
            #expect($0 == "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    @Test
    func testRerunAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRerunAtStartup"
        }
        struct RetryError: Error {}
        let firstTime = Atomic(true)
        let finished = Atomic(false)
        let failedCounter = Counter()
        let succeededCounter = Counter()
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedCounter.trigger()
                throw RetryError()
            }
            succeededCounter.trigger()
        }
        let queueName = "testRerunAtStartup\(UUID().uuidString)"
        try await self.testJobQueue(numWorkers: 4, queueName: queueName, cleanUpQueue: false) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            try await failedCounter.waitFor(count: 1)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))
        }

        #expect(firstTime.load(ordering: .relaxed) == false)
        #expect(finished.load(ordering: .relaxed) == false)

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .rerun, queueName: queueName, cleanUpQueue: true) { jobQueue in
            jobQueue.registerJob(job)
            try await succeededCounter.waitFor(count: 1)
        }
    }

    @Test
    func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let counter = Counter()
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            counter.trigger()
        }
        let sqs = SQS(
            client: self.awsClient,
            region: .euwest1,
            endpoint: self.sqsEndpoint
        )
        let queueName = "testMultipleJobQueueHandlers\(UUID().uuidString)"
        try await withSQSQueue(queueName: queueName, sqs: sqs) {
            let jobQueue = try await JobQueue(
                SQSJobQueue(
                    awsClient: self.awsClient,
                    configuration: .init(queueName: queueName, region: .euwest1, endpoint: self.sqsEndpoint),
                    logger: logger
                ),
                numWorkers: 2,
                logger: logger
            )
            jobQueue.registerJob(job)
            let jobQueue2 = try await JobQueue(
                SQSJobQueue(
                    awsClient: self.awsClient,
                    configuration: .init(queueName: queueName, region: .euwest1, endpoint: self.sqsEndpoint),
                    logger: logger
                ),
                numWorkers: 2,
                logger: logger
            )
            jobQueue2.registerJob(job)

            try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [jobQueue, jobQueue2],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: logger
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                do {
                    for i in 0..<200 {
                        try await jobQueue.push(TestParameters(value: i))
                    }
                    try await counter.waitFor(count: 200)
                    await serviceGroup.triggerGracefulShutdown()
                } catch {
                    Issue.record("\(String(reflecting: error))")
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                }
            }
        }
    }

    @Test
    func testMetadata() async throws {
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let sqs = SQS(
            client: self.awsClient,
            region: .euwest1,
            endpoint: self.sqsEndpoint
        )

        let queueName = "testMetadata\(UUID().uuidString)"
        try await withSQSQueue(queueName: queueName, sqs: sqs) {
            let jobQueue = try await SQSJobQueue(
                awsClient: self.awsClient,
                configuration: .init(queueName: queueName, region: .euwest1, endpoint: self.sqsEndpoint),
                logger: logger
            )
            let value = ByteBuffer(string: "Testing metadata")
            try await jobQueue.setMetadata(key: "test", value: value)
            let metadata = try await jobQueue.getMetadata("test")
            #expect(metadata == value)
            let value2 = ByteBuffer(string: "Testing metadata again")
            try await jobQueue.setMetadata(key: "test", value: value2)
            let metadata2 = try await jobQueue.getMetadata("test")
            #expect(metadata2 == value2)
        }
    }
}

struct Counter {
    let stream: AsyncStream<Void>
    let continuation: AsyncStream<Void>.Continuation

    init() {
        (self.stream, self.continuation) = AsyncStream.makeStream(of: Void.self)
    }

    func trigger() {
        self.continuation.yield()
    }

    func waitFor(count: Int) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                var iterator = stream.makeAsyncIterator()
                for _ in 0..<count {
                    _ = await iterator.next()
                }

            }
            group.addTask {
                try await Task.sleep(for: .seconds(5))
            }
            try await group.next()
            group.cancelAll()
        }
    }
}
