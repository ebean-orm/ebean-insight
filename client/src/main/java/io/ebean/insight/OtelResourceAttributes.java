package io.ebean.insight;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the W3C-baggage-style {@code key=value,key=value} string used by the
 * standard {@code OTEL_RESOURCE_ATTRIBUTES} env var.
 *
 * <p>Per the OpenTelemetry specification, values are URL-encoded and may
 * contain commas / equals signs after decoding. Entries with empty keys are
 * skipped. Whitespace around keys and values is trimmed.
 */
final class OtelResourceAttributes {

  private OtelResourceAttributes() {
  }

  /**
   * Parse the value of {@code OTEL_RESOURCE_ATTRIBUTES} into an ordered map.
   * Returns an empty map for null / blank input.
   */
  static Map<String, String> parse(String spec) {
    var map = new LinkedHashMap<String, String>();
    if (spec == null || spec.isBlank()) {
      return map;
    }
    for (String entry : spec.split(",")) {
      int eq = entry.indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String key = entry.substring(0, eq).trim();
      String rawValue = entry.substring(eq + 1).trim();
      if (key.isEmpty()) {
        continue;
      }
      map.put(key, decode(rawValue));
    }
    return map;
  }

  private static String decode(String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      // tolerant fallback — keep raw if not valid url-encoding
      return value;
    }
  }
}
