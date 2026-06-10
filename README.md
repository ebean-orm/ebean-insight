[![Maven Central : ebean](https://maven-badges.herokuapp.com/maven-central/io.ebean/ebean-insight/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.ebean/ebean-insight)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ebean-orm/ebean-insight/blob/master/LICENSE)

# ebean-insight

Client that collects Ebean ORM metrics and sends them to the ebean insight service.

This is currently running as a closed BETA programme.

## Usage

By default `InsightClient` acts as a *forwarder*: an upstream avaje-metrics poll
owns metric collection and feeds snapshots in, while this client forwards them on
and (with `capturePlans(true)`) captures Ebean query plans.

```java
InsightClient.builder()
    .appName("myapp")
    .environment("prod")
    .database(database)
    .capturePlans(true)
    .build()
    .register();
```

See the `InsightClient` javadoc for the forwarder / collector roles, the external
metric feed, and synchronous `lambdaMode`.

## Guides

- [AWS Lambda](docs/aws-lambda.md) — running on Lambda (and similar freeze/thaw
  runtimes) with `lambdaMode(true)`.

