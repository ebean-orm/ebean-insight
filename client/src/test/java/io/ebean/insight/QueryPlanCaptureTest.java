package io.ebean.insight;

import io.ebean.ProfileLocation;
import io.ebean.meta.MetaQueryPlan;
import io.ebean.meta.QueryPlanInit;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.ebean.insight.QueryPlanCapture.parseMessage;
import static org.assertj.core.api.Assertions.assertThat;

class QueryPlanCaptureTest {

  @Test
  void notifyListener_invokesListenerWithPlan() {
    List<MetaQueryPlan> received = new ArrayList<>();
    QueryPlanCapture capture = new QueryPlanCapture(null, null, 10, 60, received::add);

    MetaQueryPlan plan = new Plan("h1");
    capture.notifyListener(plan);

    assertThat(received).containsExactly(plan);
  }

  @Test
  void notifyListener_swallowsListenerException() {
    QueryPlanCapture capture = new QueryPlanCapture(null, null, 10, 60, p -> {
      throw new RuntimeException("boom");
    });

    // must not propagate
    capture.notifyListener(new Plan("h1"));
  }

  @Test
  void notifyListener_nullListener_noop() {
    QueryPlanCapture capture = new QueryPlanCapture(null, null, 10, 60, null);
    capture.notifyListener(new Plan("h1"));
  }

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

  static final class Plan implements MetaQueryPlan {
    private final String hash;

    Plan(String hash) {
      this.hash = hash;
    }

    @Override public Class<?> beanType() { return null; }
    @Override public String label() { return "la"; }
    @Override public ProfileLocation profileLocation() { return null; }
    @Override public String sql() { return "select 1"; }
    @Override public String hash() { return hash; }
    @Override public String bind() { return "bi"; }
    @Override public String plan() { return "pl"; }
    @Override public long queryTimeMicros() { return 0; }
    @Override public long captureCount() { return 0; }
    @Override public long captureMicros() { return 0; }
    @Override public Instant whenCaptured() { return Instant.EPOCH; }
  }
}
