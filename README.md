## AWS SQS driver for Jobs

Amazon Simple Queue Service (SQS) offers a secure, durable hosted queue. This module is a driver for [Swift Jobs](https://github.com/hummingbird-project/swift-jobs) using AWS SQS.

## Setup

The SQS job queue driver uses `AWSClient` from `Soto` to communicate with AWS.

The job queue configuration includes four values.
- `queueName`: Name of queue used to differentiate itself from other queues.
- `endpoint`: If you want to use a custom endpoint.
- `visibilityTimeoutInSeconds`: How long a job is invisible after having been read from the queue. If the job does not complete in this time another job queue handler can pick it up and attempt to complete the job.
- `pollTime`: This is the amount of time between the last time the queue was empty and the next time the driver starts looking for pending jobs.

```swift
import JobsSQS
import SotoCore

let awsClient = AWSClient(...)
let jobQueue = JobQueue(
    .sqs(
        awsClient: awsClient,
        configuration: .init(
            queueName: "MyJobQueue",
            region: .euwest1
        ),
        logger: logger
    ), 
    numWorkers: 8, 
    logger: logger
)
let serviceGroup = ServiceGroup(
    configuration: .init(
        services: [jobQueue],
        gracefulShutdownSignals: [.sigterm, .sigint],
        logger: logger
    )
)
try await serviceGroup.run()
```

## Additional Features

There are features specific to the SQS Job Queue implementation.

### Push Options

When pushing a job to the queue the following options are available.

#### Delaying jobs

As with all queue drivers you can add a delay before a job is processed. The job will sit in the pending queue and will not be available for processing until time has passed its delay until time.

```swift
// Add TestJob to the queue, but don't process it for 2 minutes
try await jobQueue.push(TestJob(), options: .init(delayUntil: .now + 120))
```
