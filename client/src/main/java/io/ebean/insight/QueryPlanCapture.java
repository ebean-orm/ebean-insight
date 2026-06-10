package io.ebean.insight;

import io.ebean.Database;
import io.ebean.meta.MetaQueryPlan;
import io.ebean.meta.QueryPlanInit;
import io.ebean.meta.QueryPlanRequest;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.System.Logger.Level.*;

final class QueryPlanCapture {

  private static final System.Logger log = InsightClient.log;

  private final Map<QueryPlanInit,Instant> pendingInit = new ConcurrentHashMap<>();
  private final Map<String, Instant> pendingCapture = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean();

  private final Database database;
  private final InsightClient client;
  private final int freqSeconds;
  private final Consumer<MetaQueryPlan> listener;

  QueryPlanCapture(Database database, InsightClient client, int freqSeconds, Consumer<MetaQueryPlan> listener) {
    this.database = database;
    this.client = client;
    this.freqSeconds = freqSeconds;
    this.listener = listener;
  }

  void start() {
    database.backgroundExecutor().scheduleAtFixedRate(this::progress, freqSeconds, freqSeconds, TimeUnit.SECONDS);
  }

  void process(String rawMessage) {
    QueryPlanInit init = parseMessage(rawMessage);
    initiatePlanCapture(init);
    if (!init.isEmpty()) {
      pendingInit.put(init, Instant.now());
    }
  }

  private boolean hasPending() {
    if (pendingCapture.isEmpty()) {
      return false;
    }
    Instant firstPending = pendingCapture.values().stream()
      .min(Instant::compareTo)
      .orElse(Instant.now());

    return firstPending.isBefore(Instant.now().minusSeconds(60));
  }

  /**
   * Advance the query-plan capture state machine once: initialise pending
   * captures, and if any have been armed long enough, collect and send them.
   * <p>
   * Driven by the background scheduler in normal mode, or inline from
   * {@link InsightClient#accept} in lambdaMode. A single-flight guard drops an
   * overlapping call (the next cycle picks the work up) so concurrent callers
   * never double-collect/double-send.
   */
  void progress() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      pendingCaptureInitialisation();
      if (!hasPending()) {
        return;
      }

      QueryPlanRequest request = new QueryPlanRequest();
      request.maxCount(10);
      request.maxTimeMillis(10_000);

      List<MetaQueryPlan> capturedPlans = database.metaInfo().queryPlanCollectNow(request);
      if (!capturedPlans.isEmpty()) {
        for (MetaQueryPlan metaQueryPlan : capturedPlans) {
          Instant whenInitiated = pendingCapture.remove(metaQueryPlan.hash());
          log.log(DEBUG, "Query plan captured for {0} initialised:{1}", metaQueryPlan.hash(), whenInitiated);
          notifyListener(metaQueryPlan);
        }
        sendPlans(capturedPlans);
      }
      int stillPending = pendingCapture.size();
      if (stillPending > 0) {
        log.log(DEBUG, "{0} Pending query plan capture for plans - {1}", stillPending, pendingCapture.keySet());
      }
    } catch (Exception e) {
      log.log(WARNING, "Error during query plan capture", e);
    } finally {
      running.set(false);
    }
  }

  private void sendPlans(List<MetaQueryPlan> plans) {
    client.sendPlans(plans);
  }

  void notifyListener(MetaQueryPlan plan) {
    if (listener != null) {
      try {
        listener.accept(plan);
      } catch (Exception e) {
        log.log(WARNING, "Error in query plan capture listener for: " + plan.hash(), e);
      }
    }
  }

  private void pendingCaptureInitialisation() {
    Iterator<Map.Entry<QueryPlanInit, Instant>> iterator = pendingInit.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<QueryPlanInit, Instant> entry = iterator.next();
      QueryPlanInit pendingInit = entry.getKey();
      initiatePlanCapture(pendingInit);
      if (pendingInit.isEmpty()) {
        iterator.remove();
      }
    }
  }

  void initiatePlanCapture(QueryPlanInit planInit) {
    var initialisedPlans = database.metaInfo().queryPlanInit(planInit);
    for (MetaQueryPlan metaQueryPlan : initialisedPlans) {
      planInit.remove(metaQueryPlan.hash());
      pendingCapture.put(metaQueryPlan.hash(), Instant.now());
      log.log(DEBUG, "Initialised query plan capture for {0} {1}", metaQueryPlan.hash(), metaQueryPlan.label());
    }
  }

  static QueryPlanInit parseMessage(String rawMessage) {
    var queryPlanInit = new QueryPlanInit();
    if (rawMessage == null || rawMessage.isEmpty()) {
      return queryPlanInit;
    }
    // version|messages ...
    String[] split = rawMessage.split("\\|");
    if (split.length < 2 || !"v1".equals(split[0])) {
      return queryPlanInit;
    }
    for (String msg : split) {
      parseMessage(msg, queryPlanInit);
    }
    return queryPlanInit;
  }

  static void parseMessage(String msg, QueryPlanInit queryPlanInit) {
    try {
      if (msg.startsWith("qp:")) {
        String sub = msg.substring(3);
        String[] split = sub.split(":");
        if (split.length == 1) {
          queryPlanInit.add(split[0], 0);
        } else if (split.length == 2) {
          queryPlanInit.add(split[1], Long.parseLong(split[0]));
        }
      } else if (msg.startsWith("th:")) {
        queryPlanInit.thresholdMicros(Long.parseLong(msg.substring(3)));
      }
    } catch (Exception e) {
      log.log(ERROR, "Error processing msg line: " + msg, e);
    }
  }

}
