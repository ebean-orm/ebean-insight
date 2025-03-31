package io.ebean.insight;

import org.junit.jupiter.api.Test;

import static io.ebean.insight.JsonEscape.escape;
import static org.junit.jupiter.api.Assertions.*;

class EscapeJsonTest {

  @Test
  void basic() {
    assertEquals("hi", escape("hi"));
  }

  @Test
  void quote() {
    assertEquals("hi\\\"there", escape("hi\"there"));
  }

  @Test
  void slash() {
    assertEquals("hi\\\\there", escape("hi\\there"));
  }

  @Test
  void nrbtf() {
    assertEquals("1\\n2\\r3\\b4\\t5\\f6", escape("1\n2\r3\b4\t5\f6"));
  }
}
