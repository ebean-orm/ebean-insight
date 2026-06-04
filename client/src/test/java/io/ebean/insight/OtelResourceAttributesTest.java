package io.ebean.insight;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OtelResourceAttributesTest {

  @Test
  void emptyOrNull() {
    assertTrue(OtelResourceAttributes.parse(null).isEmpty());
    assertTrue(OtelResourceAttributes.parse("").isEmpty());
    assertTrue(OtelResourceAttributes.parse("   ").isEmpty());
  }

  @Test
  void singleEntry() {
    Map<String, String> m = OtelResourceAttributes.parse("service.namespace=consolidation");
    assertEquals(1, m.size());
    assertEquals("consolidation", m.get("service.namespace"));
  }

  @Test
  void multipleEntries() {
    Map<String, String> m = OtelResourceAttributes.parse(
      "service.namespace=consolidation,business.domain=ingestion,business.system=consolidation");
    assertEquals(3, m.size());
    assertEquals("consolidation", m.get("service.namespace"));
    assertEquals("ingestion", m.get("business.domain"));
    assertEquals("consolidation", m.get("business.system"));
  }

  @Test
  void trimsWhitespace() {
    Map<String, String> m = OtelResourceAttributes.parse("  k1 = v1 ,  k2= v2");
    assertEquals("v1", m.get("k1"));
    assertEquals("v2", m.get("k2"));
  }

  @Test
  void urlDecodesValues() {
    Map<String, String> m = OtelResourceAttributes.parse("k=hello%20world,k2=a%2Cb");
    assertEquals("hello world", m.get("k"));
    assertEquals("a,b", m.get("k2"));
  }

  @Test
  void skipsMalformedEntries() {
    Map<String, String> m = OtelResourceAttributes.parse("good=v1,broken,=novalue,k2=v2");
    assertEquals(2, m.size());
    assertEquals("v1", m.get("good"));
    assertEquals("v2", m.get("k2"));
  }
}
