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

import static java.lang.System.Logger.Level.*;

final class QueryPlanCapture {

  private static final System.Logger log = InsightClient.log;

  private final Map<QueryPlanInit,Instant> pendingInit = new ConcurrentHashMap<>();
  private final Map<String, Instant> pendingCapture = new ConcurrentHashMap<>();

  private final Database database;
  private final InsightClient client;
  private final int freqSeconds;

  QueryPlanCapture(Database database, InsightClient client, int freqSeconds) {
    this.database = database;
    this.client = client;
    this.freqSeconds = freqSeconds;
  }

  void start() {
    database.backgroundExecutor().scheduleAtFixedRate(this::periodic, freqSeconds, freqSeconds, TimeUnit.SECONDS);
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

  private void periodic() {
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
          log.log(INFO, "Query plan captured for {0} initialised:{1}", metaQueryPlan.hash(), whenInitiated);
        }
        sendPlans(capturedPlans);
      }
      int stillPending = pendingCapture.size();
      if (stillPending > 0) {
        log.log(INFO, "{0} Pending query plan capture for plans - {1}", stillPending, pendingCapture.keySet());
      }
    } catch (Exception e) {
      log.log(WARNING, "Error during query plan capture", e);
    }
  }

  private void sendPlans(List<MetaQueryPlan> plans) {
    client.sendPlans(plans);
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
      log.log(INFO, "Initialised query plan capture for {0} {1}", metaQueryPlan.hash(), metaQueryPlan.label());
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
