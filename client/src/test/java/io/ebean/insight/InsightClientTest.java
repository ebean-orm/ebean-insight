package io.ebean.insight;


import io.avaje.config.Config;
import io.avaje.metrics.Metrics;
import io.ebean.ProfileLocation;
import io.ebean.meta.MetaQueryPlan;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InsightClientTest {

  @Disabled
  @Test
  void create() throws InterruptedException {

    Metrics.jvmMetrics().registerJvmMetrics();

    final InsightClient client = InsightClient.builder()
      //.url("http://localhost:8090")
      .periodSecs(5)
      .appName("test")
      .environment("local")
      .instanceId("1")
      .key("YeahNah")
      .ping(false)
      .gzip(false)
      .timeoutSecs(10)
      .collectEbeanMetrics(false)
      .build();

    assertTrue(client.isActive());
    Thread.sleep(40_000);
  }

  @Disabled
  @Test
  void pingFailed_expect_notStarted() throws InterruptedException {

    Metrics.jvmMetrics().registerJvmMetrics();

    final InsightClient client = InsightClient.builder()
      .url("http://doesNotExist:8090")
      .periodSecs(1)
      .appName("test")
      .environment("local")
      .instanceId("1")
      .key("YeahNah")
      .timeoutSecs(10)
      .collectEbeanMetrics(false)
      .build();

    assertFalse(client.isActive());
    assertFalse(client.ping());

    Thread.sleep(5_000);
  }

  @Test
  void notEnabled_when_keyNotValid() {

    // not valid keys
    assertThat(InsightClient.builder().key(null).enabled()).isFalse();
    assertThat(InsightClient.builder().key("").enabled()).isFalse();
    assertThat(InsightClient.builder().key("  ").enabled()).isFalse();
    assertThat(InsightClient.builder().key("none").enabled()).isFalse();

    // valid
    assertThat(InsightClient.builder().key("foo").enabled()).isTrue();
  }

  @Test
  void notEnabled_when_systemPropertySet() {
    assertThat(InsightClient.builder().key("foo").enabled()).isTrue();

    Config.setProperty("ebean.insight.enabled", "false");
    assertThat(InsightClient.builder().key("foo").enabled()).isFalse();

    Config.setProperty("ebean.insight.enabled", "true");
    assertThat(InsightClient.builder().key("foo").enabled()).isTrue();
  }

  @Test
  void enabled() {
    assertThat(InsightClient.builder().key("foo").enabled(true).enabled()).isTrue();
    assertThat(InsightClient.builder().key("foo").enabled(false).enabled()).isFalse();
    assertThat(InsightClient.builder().key("foo").enabled()).isTrue();
  }

  @Test
  void buildJsonContent() {
    InsightClient client = InsightClient.builder()
      .collectEbeanMetrics(false)
      .collectAvajeMetrics(false)
      .build();
    String jsonContent = client.buildJsonContent();

    assertThat(jsonContent).contains("{\"eventTime\":");
    assertThat(jsonContent).contains(",\"startEventTime\":");
    assertThat(jsonContent).contains(" ,\"collect\":");
    assertThat(jsonContent).doesNotContain(" ,\"metrics\":[");
  }

  @Test
  void buildJsonContent_startEventTimeAdvances() throws Exception {
    InsightClient client = InsightClient.builder()
      .collectEbeanMetrics(false)
      .collectAvajeMetrics(false)
      .build();

    String first = client.buildJsonContent();
    Thread.sleep(2);
    String second = client.buildJsonContent();

    long firstEvent = extractLong(first, "\"eventTime\":");
    long firstStart = extractLong(first, "\"startEventTime\":");
    long secondEvent = extractLong(second, "\"eventTime\":");
    long secondStart = extractLong(second, "\"startEventTime\":");

    // start() (called from build()) primes lastEventTime, so first window has a real start.
    assertThat(firstStart).isGreaterThan(0L);
    assertThat(firstEvent).isGreaterThanOrEqualTo(firstStart);
    // Second call's window starts where the first call ended -> disjoint windows.
    assertThat(secondStart).isEqualTo(firstEvent);
    assertThat(secondEvent).isGreaterThanOrEqualTo(firstEvent);
  }

  private static long extractLong(String json, String key) {
    int i = json.indexOf(key) + key.length();
    int j = i;
    while (j < json.length() && (Character.isDigit(json.charAt(j)) || json.charAt(j) == '-')) {
      j++;
    }
    return Long.parseLong(json.substring(i, j));
  }

  @Test
  void buildPlanJsonContent() {
    InsightClient client = InsightClient.builder()
      .environment("e")
      .appName("a")
      .build();

    Instant when = Instant.parse("2025-01-02T03:04:05.123Z");

    var onePlan = List.of(
      newPlan("select 1", "abc", when));

    String onePlanJson = client.buildPlansJson(onePlan);
    assertThat(onePlanJson).isEqualTo("{\"environment\":\"e\" ,\"appName\":\"a\" ,\"plans\":[{\"hash\":\"abc\" ,\"whenCaptured\":\"2025-01-02T03:04:05.123Z\" ,\"label\":\"la\" ,\"queryTimeMicros\":0 ,\"captureMicros\":0 ,\"captureCount\":0 ,\"bind\":\"bi\" ,\"plan\":\"pl\" ,\"sql\":\"select 1\"}]}");

    var twoPlans = List.of(
      newPlan("select 1", "abc", when),
      newPlan("select 2", "def", when));

    String twoPlansJson = client.buildPlansJson(twoPlans);
    assertThat(twoPlansJson).isEqualTo("{\"environment\":\"e\" ,\"appName\":\"a\" ,\"plans\":[{\"hash\":\"abc\" ,\"whenCaptured\":\"2025-01-02T03:04:05.123Z\" ,\"label\":\"la\" ,\"queryTimeMicros\":0 ,\"captureMicros\":0 ,\"captureCount\":0 ,\"bind\":\"bi\" ,\"plan\":\"pl\" ,\"sql\":\"select 1\"},{\"hash\":\"def\" ,\"whenCaptured\":\"2025-01-02T03:04:05.123Z\" ,\"label\":\"la\" ,\"queryTimeMicros\":0 ,\"captureMicros\":0 ,\"captureCount\":0 ,\"bind\":\"bi\" ,\"plan\":\"pl\" ,\"sql\":\"select 2\"}]}");
  }

  @Test
  void resourceAttributes_backfillEnvironment_whenNotSetExplicitly() {
    InsightClient client = InsightClient.builder()
      .appName("a")
      .resourceAttributes("deployment.environment.name=test")
      .build();

    String json = client.buildPlansJson(List.of(
      newPlan("select 1", "abc", Instant.parse("2025-01-02T03:04:05.123Z"))));

    assertThat(json).contains("\"environment\":\"test\"");
  }

  @Test
  void resourceAttributes_backfillEnvironment_legacyKey() {
    InsightClient client = InsightClient.builder()
      .appName("a")
      .resourceAttributes("deployment.environment=test")
      .build();

    String json = client.buildPlansJson(List.of(
      newPlan("select 1", "abc", Instant.parse("2025-01-02T03:04:05.123Z"))));

    assertThat(json).contains("\"environment\":\"test\"");
  }

  @Test
  void explicitEnvironment_winsOverResourceAttributes() {
    InsightClient client = InsightClient.builder()
      .appName("a")
      .environment("prod")
      .resourceAttributes("deployment.environment.name=test")
      .build();

    String json = client.buildPlansJson(List.of(
      newPlan("select 1", "abc", Instant.parse("2025-01-02T03:04:05.123Z"))));

    assertThat(json).contains("\"environment\":\"prod\"");
  }

  private MetaQueryPlan newPlan(String sql, String hash, Instant whenCaptured) {
    return new Plan(sql, hash, whenCaptured);
  }

  static class Plan implements MetaQueryPlan {

    private final String sql;
    private final String hash;
    private final Instant whenCaptured;

    Plan(String sql, String hash, Instant whenCaptured) {
      this.sql = sql;
      this.hash = hash;
      this.whenCaptured = whenCaptured;
    }

    @Override
    public Class<?> beanType() {
      return null;
    }

    @Override
    public String label() {
      return "la";
    }

    @Override
    public ProfileLocation profileLocation() {
      return null;
    }

    @Override
    public String sql() {
      return sql;
    }

    @Override
    public String hash() {
      return hash;
    }

    @Override
    public String bind() {
      return "bi";
    }

    @Override
    public String plan() {
      return "pl";
    }

    @Override
    public long queryTimeMicros() {
      return 0;
    }

    @Override
    public long captureCount() {
      return 0;
    }

    @Override
    public long captureMicros() {
      return 0;
    }

    @Override
    public Instant whenCaptured() {
      return whenCaptured;
    }
  }
}
