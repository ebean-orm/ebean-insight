# Guide: ebean-insight on AWS Lambda

This guide covers running `InsightClient` inside **AWS Lambda** (or any similar
freeze-on-exit serverless runtime) using **`lambdaMode(true)`**.

For the general forwarder/collector model see the `InsightClient` javadoc; for the
companion avaje-metrics OpenTelemetry Lambda recipe (the `ScheduledTask` /
`TelemetryWaiter` `waitIfRunning()` machinery) see the avaje-metrics
`add-open-telemetry-lambda` guide.

## Why Lambda needs a different mode

AWS Lambda **freezes the worker** the moment the handler returns and **thaws** it
for the next invocation. Two things in the default `InsightClient` are unsafe there:

1. **Background timers.** The internal metric `Timer` and the `QueryPlanCapture`
   background poll run on their own threads. Once the worker freezes those threads
   stop; a low-traffic Lambda may never let them tick.
2. **Async HTTP callbacks.** Metric/plan POSTs default to `HttpClient.sendAsync(...)`
   and handle the response (which carries the server's plan-capture directives) on
   an HttpClient thread **after** the call site returns — so the response can be
   lost to a freeze before it is processed.

`lambdaMode(true)` removes both: **no background threads**, all I/O is
**synchronous on the calling thread**, and query-plan capture is advanced
**inline** from `accept(ServerMetrics)` each report cycle. Because the work runs on
the invocation thread, your handler's existing `waitIfRunning()` drain already
covers insight reporting — no extra waiter is needed.

## Dependencies

`register()` (below) needs `avaje-metrics-ebean` on the classpath; it is a
non-optional dependency of `ebean-insight` so it is pulled in transitively. You
already have Ebean and avaje-metrics in a Lambda that reports metrics.

```xml
<dependency>
  <groupId>io.ebean</groupId>
  <artifactId>ebean-insight</artifactId>
  <version>LATEST</version>
</dependency>
```

## Setup

Build the `InsightClient` **once** when the handler class is loaded (Lambda reuses
the same handler instance across invocations on a warm worker) and **register** it
as a forwarder against the same avaje-metrics registry your Lambda already reports
from.

```java
public class ConsolidationHandler implements RequestHandler<SQSEvent, Void> {

  // built once per warm worker, reused across invocations
  private static final InsightClient INSIGHT =
      InsightClient.builder()
          .appName("consolidation")
          .environment("prod")
          .database(database)
          .capturePlans(true)
          .lambdaMode(true)            // no background threads, synchronous I/O
          .build()
          .register();                 // forward metrics + drive plan capture

  ...
}
```

`build()` starts the client (no timers are scheduled in lambdaMode). `register()`
wires a `DatabaseMetricSupplier` per registered database that forwards every
reset-on-read snapshot to the client via `accept(ServerMetrics)`.

`collectEbeanMetrics` / `collectAvajeMetrics` default to **false** — leave them so
the client never tries to poll on its own; collection is owned by the upstream
avaje-metrics registry poll.

## How it is driven

In lambdaMode the client does nothing on its own — it must be *driven* by a registry
collection. Whatever already collects your avaje-metrics registry on each invocation
(an avaje-metrics `ScheduledTask` reporter, the avaje-metrics-otel periodic reader,
etc.) now also drives insight, because `register()` adds the forwarder to that same
registry:

```
registry collected  →  DatabaseMetricSupplier.collectMetrics()  (reset-on-read poll)
                    →  forwardTo  →  InsightClient.accept(snapshot)
                                        →  synchronous POST to insight-server
                                        →  inline QueryPlanCapture.progress()
```

Every step runs on the thread that performed the collection. Keep that collection
inside the part of the invocation covered by `waitIfRunning()` so it completes
before the worker freezes:

```java
@Timed(prefix = "lambda", span = Timed.SpanMode.ROOT)
public Void handleRequest(SQSEvent event, Context context) {
  try {
    consolidateService.consolidate(convert(event));
  } finally {
    // drains any in-flight registry report (which includes the synchronous
    // insight POST + inline plan progress) before the worker freezes
    scheduledTask.waitIfRunning(2, SECONDS);
    telemetryWaiter.waitIfRunning(); // if also exporting to OpenTelemetry
  }
  return null;
}
```

If you have **no** existing reporter, collect the registry yourself once per
invocation (still inside the handler so it completes before freeze):

```java
metricRegistry.collectMetrics(); // triggers the forwarder → insight POST
```

## Query plan capture on Lambda

Plan capture is an inherently multi-step, time-delayed protocol and is therefore
**best-effort** on Lambda:

1. A metrics POST response arms a capture for a slow query's hash.
2. The application must **re-execute that query** so Ebean buffers its execution
   plan — on the **same warm worker**.
3. After `captureDelaySeconds` (default 60) a later `accept()` cycle harvests the
   plan and POSTs it.

The pending state lives on the heap, which a warm worker preserves across
freeze/thaw, so capture works when traffic keeps the same worker alive across the
window. Sparse traffic or a scale-down can leave a capture incomplete — it simply
re-arms next time the query is seen.

Tune the arm→harvest window for Lambda with `captureDelaySeconds(...)`. A shorter
delay collects sooner (helpful when workers are short-lived) at the risk of
harvesting before the query has re-run:

```java
InsightClient.builder()
    .database(database)
    .capturePlans(true)
    .lambdaMode(true)
    .captureDelaySeconds(15)   // default 60
    .build()
    .register();
```

Note: plan capture targets the **first** registered database only.

## Configuration reference

All builder options have matching properties (resolved via avaje-config):

| Property | Default | Notes |
|---|---|---|
| `ebean.insight.lambdaMode` | `false` | Synchronous, no-background-thread mode. |
| `ebean.insight.queryPlan.captureDelaySecs` | `60` | Arm→harvest window for plan capture. |
| `ebean.insight.collectEbeanMetrics` | `false` | Keep false on Lambda (forwarder role). |
| `ebean.insight.collectAvajeMetrics` | `false` | Keep false on Lambda (forwarder role). |
| `ebean.insight.enabled` | `true` | Master on/off. |

## Checklist

- [ ] `lambdaMode(true)` set (or `ebean.insight.lambdaMode=true`).
- [ ] `InsightClient` built **once** at handler class-init, not per invocation.
- [ ] `register()` called so the forwarder is on the reporting registry.
- [ ] `collectEbeanMetrics` / `collectAvajeMetrics` left `false`.
- [ ] The registry is collected each invocation and drained by `waitIfRunning()`
      in a `finally` block before the handler returns.
