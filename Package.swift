// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-jobs-sqs",
    platforms: [.macOS(.v15), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "JobsSQS", targets: ["JobsSQS"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.0.0-beta.8"),
        .package(url: "https://github.com/soto-project/soto-core.git", from: "7.4.0"),
        .package(url: "https://github.com/soto-project/soto-codegenerator.git", from: "7.6.0"),
    ],
    targets: [
        .target(
            name: "JobsSQS",
            dependencies: [
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "SotoCore", package: "soto-core"),
            ],
            plugins: [.plugin(name: "SotoCodeGeneratorPlugin", package: "soto-codegenerator")]
        ),
        .testTarget(
            name: "JobsSQSTests",
            dependencies: ["JobsSQS"]
        ),
    ]
)
