//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
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
    let awsClient = AWSClient()

    deinit {
        try? awsClient.syncShutdown()
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function and ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        //failedJobsInitialization: SQSJobQueue.JobCleanup = .remove,
        queueName: String = #function,
        test: (JobQueue<SQSJobQueue>) async throws -> T
    ) async throws -> T {
        let queueName = queueName.filter(\.isLetter) + UUID().uuidString
        var logger = Logger(label: "SQSJobsTests")
        logger.logLevel = .debug
        let sqs = SQS(client: self.awsClient, endpoint: "http://localhost:4566")
        let jobQueue = try await JobQueue(
            .sqs(
                sqs: sqs,
                configuration: .init(queueName: queueName),
                logger: logger
            ),
            numWorkers: numWorkers,
            logger: logger,
            options: .init(
                defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10))
            )
        )

        let value: T
        do {
            value = try await withThrowingTaskGroup(of: Void.self) { group in
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
                //try await jobQueue.queue.cleanup(failedJobs: failedJobsInitialization, processingJobs: .remove, pendingJobs: .remove)
                let value = try await test(jobQueue)
                await serviceGroup.triggerGracefulShutdown()
                return value
            }
        } catch {
            guard let queueURL = try? await sqs.getQueueUrl(queueName: "\(queueName)").queueUrl else { throw error }
            try? await sqs.deleteQueue(queueUrl: queueURL)
            throw error
        }
        guard let queueURL = try? await sqs.getQueueUrl(queueName: "\(queueName)").queueUrl else { return value }
        try? await sqs.deleteQueue(queueUrl: queueURL)
        return value
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

            await counter.waitFor(count: 10)
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

            await counter.waitFor(count: 10)

            #expect(maxRunningJobCounter.load(ordering: .relaxed) > 1)
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

            await counter.waitFor(count: 4)

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

            await counter.waitFor(count: 2)

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

            await counter.waitFor(count: 1)
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
                options: .init(delayUntil: Date.now.addingTimeInterval(1))
            )
            try await jobQueue.push(TestParameters(value: 50))
            try await jobQueue.push(TestParameters(value: 10))

            await counter.waitFor(count: 3)
        }
        jobExecutionSequence.withLock {
            #expect($0 == [50, 10, 100])
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
            await counter.waitFor(count: 1)
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
            await counter.waitFor(count: 1)
        }
        string.withLock {
            #expect($0 == "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    /* Need the failed queue before we can continue with this
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
            finished.store(true, ordering: .relaxed)
        }
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            await failedCounter.waitFor(count: 1)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            #expect(firstTime.load(ordering: .relaxed))
            #expect(finished.load(ordering: .relaxed))
        }

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .rerun) { jobQueue in
            jobQueue.registerJob(job)
            await succeededCounter.waitFor(count: 1)
            #expect(finished.load(ordering: .relaxed))
        }
    }
*/
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
        let queueName = "testMultipleJobQueueHandlers" + UUID().uuidString
        let sqs = SQS(client: self.awsClient, endpoint: "http://localhost:4566")
        let jobQueue = try await JobQueue(
            SQSJobQueue(sqs: sqs, configuration: .init(queueName: queueName), logger: logger),
            numWorkers: 2,
            logger: logger
        )
        jobQueue.registerJob(job)
        let jobQueue2 = try await JobQueue(
            SQSJobQueue(sqs: sqs, configuration: .init(queueName: queueName), logger: logger),
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
                await counter.waitFor(count: 200)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                Issue.record("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }
    /*
    func testMetadata() async throws {
        let redis = try createRedisConnectionPool(logger: Logger(label: "Jobs"))
        let jobQueue = SQSJobQueue(redis)
        let value = ByteBuffer(string: "Testing metadata")
        try await jobQueue.setMetadata(key: "test", value: value)
        let metadata = try await jobQueue.getMetadata("test")
        XCTAssertEqual(metadata, value)
        let value2 = ByteBuffer(string: "Testing metadata again")
        try await jobQueue.setMetadata(key: "test", value: value2)
        let metadata2 = try await jobQueue.getMetadata("test")
        XCTAssertEqual(metadata2, value2)
    }*/
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

    func waitFor(count: Int) async {
        var iterator = stream.makeAsyncIterator()
        for _ in 0..<count {
            _ = await iterator.next()
        }
    }
}
