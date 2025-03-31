package io.ebean.insight;

/**
 * Internal helper to write json content.
 */
final class JsonSimple {

  private boolean keyPrefix;

  private final StringBuilder buffer;

  JsonSimple() {
    this.buffer = new StringBuilder(1000);
  }

  void begin(char start) {
    buffer.append(start);
    keyPrefix = false;
  }
  void end(char end) {
    buffer.append(end);
    keyPrefix = false;
  }

  void key(String key) {
    preKey();
    str(key);
    buffer.append(':');
  }

  void keyVal(String key, String val) {
    if (val != null) {
      preKey();
      str(key);
      buffer.append(':');
      str(val);
    }
  }

  void keyVal(String key, long val) {
    preKey();
    str(key);
    buffer.append(':');
    buffer.append(val);
  }

  void keyValEscape(String key, String val) {
    preKey();
    str(key);
    buffer.append(':');
    buffer.append('"');
    buffer.append(JsonEscape.escape(val));
    buffer.append('"');
  }

  private void preKey() {
    if (keyPrefix) {
      buffer.append(" ,");
    } else {
      keyPrefix = true;
    }
  }

  private void str(String key) {
    buffer.append('"').append(key).append('"');
  }

  void append(String raw) {
    buffer.append(raw);
  }

  void append(char raw) {
    buffer.append(raw);
  }

  String asJson() {
    return buffer.toString();
  }

  StringBuilder buffer() {
    return buffer;
  }

  @Override
  public String toString() {
    return asJson();
  }

}
