package io.ebean.insight;


import io.avaje.metrics.MetricManager;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InsightClientTest {

  @Ignore
  @Test
  public void create() throws InterruptedException {

    MetricManager.jvmMetrics()
      .registerJvmMetrics()
      .registerCGroupMetrics()
      .registerLogbackMetrics();

    InsightClient.create()
      .url("http://localhost:8090")
      .periodSecs(5)
      .appName("test")
      .environment("local")
      .instanceId("1")
      .key("YeahNah")
      //.gzip(false)
      .collectEbeanMetrics(false)
      .build();

    Thread.sleep(40_000);
  }

  @Test
  public void podService() {

    InsightClient.Builder builder = InsightClient.create();
    assertThat(builder.podService("metrics-test-7d6d5bdf8-bsvpl")).isEqualTo("metrics-test");
    assertThat(builder.podService("a-7d6d5bdf8-bsvpl")).isEqualTo("a");
    assertThat(builder.podService("7d6d5bdf8-bsvpl")).isNull();
  }
}
