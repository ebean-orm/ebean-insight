package io.ebean.insight;


import io.avaje.config.Config;
import io.avaje.metrics.MetricManager;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InsightClientTest {

  @Disabled
  @Test
  void create() throws InterruptedException {

    MetricManager.jvmMetrics()
      .registerJvmMetrics()
      .registerLogbackMetrics();

    final InsightClient client = InsightClient.create()
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

    MetricManager.jvmMetrics()
      .registerJvmMetrics();

    final InsightClient client = InsightClient.create()
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
    assertThat(InsightClient.create().key(null).enabled()).isFalse();
    assertThat(InsightClient.create().key("").enabled()).isFalse();
    assertThat(InsightClient.create().key("  ").enabled()).isFalse();
    assertThat(InsightClient.create().key("none").enabled()).isFalse();

    // valid
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }

  @Test
  void notEnabled_when_systemPropertySet() {
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();

    Config.setProperty("ebean.insight.enabled", "false");
    assertThat(InsightClient.create().key("foo").enabled()).isFalse();

    Config.setProperty("ebean.insight.enabled", "true");
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }

  @Test
  void enabled() {
    assertThat(InsightClient.create().key("foo").enabled(true).enabled()).isTrue();
    assertThat(InsightClient.create().key("foo").enabled(false).enabled()).isFalse();
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }

  @Test
  void buildJsonContent() {
    InsightClient client = InsightClient.create()
      .collectEbeanMetrics(false)
      .collectAvajeMetrics(false)
      .build();
    String jsonContent = client.buildJsonContent();

    assertThat(jsonContent).contains("{\"eventTime\":");
    assertThat(jsonContent).contains(" ,\"collect\":");
    assertThat(jsonContent).doesNotContain(" ,\"metrics\":[");
  }
}
