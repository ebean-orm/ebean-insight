package io.ebean.insight;


import io.avaje.metrics.MetricManager;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InsightClientTest {

  @Ignore
  @Test
  public void create() throws InterruptedException {

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
      //.gzip(false)
      .collectEbeanMetrics(false)
      .build();

    assertTrue(client.isActive());

    Thread.sleep(40_000);
  }

  @Ignore
  @Test
  public void pingFailed_expect_notStarted() throws InterruptedException {

    MetricManager.jvmMetrics()
      .registerJvmMetrics();

    final InsightClient client = InsightClient.create()
      .url("http://doesNotExist:8090")
      .periodSecs(1)
      .appName("test")
      .environment("local")
      .instanceId("1")
      .key("YeahNah")
      .collectEbeanMetrics(false)
      .build();

    assertFalse(client.isActive());
    assertFalse(client.ping());

    Thread.sleep(5_000);
  }

  @Test
  public void notEnabled_when_keyNotValid() {

    // not valid keys
    assertThat(InsightClient.create().key(null).enabled()).isFalse();
    assertThat(InsightClient.create().key("").enabled()).isFalse();
    assertThat(InsightClient.create().key("  ").enabled()).isFalse();
    assertThat(InsightClient.create().key("none").enabled()).isFalse();

    // valid
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }

  @Test
  public void notEnabled_when_systemPropertySet() {

    assertThat(InsightClient.create().key("foo").enabled()).isTrue();

    System.setProperty("ebean.insight.enabled", "false");
    assertThat(InsightClient.create().key("foo").enabled()).isFalse();

    System.setProperty("ebean.insight.enabled", "true");
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();

    System.clearProperty("ebean.insight.enabled");
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }

  @Test
  public void enabled() {
    assertThat(InsightClient.create().key("foo").enabled(true).enabled()).isTrue();
    assertThat(InsightClient.create().key("foo").enabled(false).enabled()).isFalse();
    assertThat(InsightClient.create().key("foo").enabled()).isTrue();
  }
}
