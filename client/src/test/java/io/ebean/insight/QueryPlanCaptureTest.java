package io.ebean.insight;

import io.ebean.meta.QueryPlanInit;
import org.junit.jupiter.api.Test;

import static io.ebean.insight.QueryPlanCapture.parseMessage;
import static org.assertj.core.api.Assertions.assertThat;

class QueryPlanCaptureTest {

  @Test
  void testParseMessage_allQueryPlans() {
    QueryPlanInit init = parseMessage("v1|qp:all");
    assertThat(init.isEmpty()).isFalse();
    assertThat(init.isAll()).isTrue();
    assertThat(init.thresholdMicros()).isEqualTo(0L);
  }

  @Test
  void testParseMessage_allQueryPlansWithThreshold() {
    QueryPlanInit init = parseMessage("v1|qp:500:all");
    assertThat(init.isEmpty()).isFalse();
    assertThat(init.isAll()).isTrue();
    assertThat(init.thresholdMicros()).isEqualTo(500L);
  }

  @Test
  void testParseMessage_thresholdAndPlans() {
    QueryPlanInit init = parseMessage("v1|th:700|qp:200:hash1|qp:hash2|qp:400:hash3");
    assertThat(init.isEmpty()).isFalse();
    assertThat(init.isAll()).isFalse();
    assertThat(init.thresholdMicros()).isEqualTo(700L);

    assertThat(init.includeHash("junk")).isFalse();
    assertThat(init.includeHash("hash1")).isTrue();
    assertThat(init.includeHash("hash2")).isTrue();
    assertThat(init.includeHash("hash3")).isTrue();
    assertThat(init.thresholdMicros("hash1")).isEqualTo(200L);
    assertThat(init.thresholdMicros("hash2")).isEqualTo(700L);
    assertThat(init.thresholdMicros("hash3")).isEqualTo(400L);
  }

  @Test
  void testParseMessage_multi() {
    QueryPlanInit init = parseMessage("v1|qp:100:myHash1|qp:myHash2|qp:200:myHash3");
    assertThat(init.isEmpty()).isFalse();
    assertThat(init.isAll()).isFalse();

    assertThat(init.includeHash("junk")).isFalse();
    assertThat(init.includeHash("myHash1")).isTrue();
    assertThat(init.includeHash("myHash2")).isTrue();
    assertThat(init.includeHash("myHash3")).isTrue();
    assertThat(init.thresholdMicros("myHash1")).isEqualTo(100L);
    assertThat(init.thresholdMicros("myHash2")).isEqualTo(0L);
    assertThat(init.thresholdMicros("myHash3")).isEqualTo(200L);
  }
}
