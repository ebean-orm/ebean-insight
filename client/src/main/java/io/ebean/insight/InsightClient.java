package io.ebean.insight;

import io.avaje.config.Config;
import io.avaje.metrics.MetricManager;
import io.ebean.DB;
import io.ebean.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.GZIPOutputStream;

import static java.net.http.HttpRequest.BodyPublishers.ofByteArray;
import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Client that collects Ebean metrics and Avaje metrics for
 * sending to the monitoring service.
 */
public class InsightClient {

  private static final Logger log = LoggerFactory.getLogger(InsightClient.class);

  private final boolean enabled;
  private final String key;
  private final String environment;
  private final String appName;
  private final String instanceId;
  private final String version;
  private final URI ingestUri;
  private final String pingUrl;
  private final long periodSecs;
  private final boolean gzip;
  private final boolean collectEbeanMetrics;
  private final boolean collectAvajeMetrics;
  private final List<Database> databaseList = new ArrayList<>();
  private final Timer timer;
  private final HttpClient httpClient;
  private final int timeoutSecs;
  private final boolean ping;
  private boolean active;
  private long contentLength;
  private long latencyMillis;
  private long collectMicros;
  private long reportMicros;

  public static InsightClient.Builder create() {
    return new InsightClient.Builder();
  }

  private InsightClient(Builder builder) {
    this.enabled = builder.enabled();
    this.ingestUri = URI.create(builder.url + "/api/ingest/metrics");
    this.pingUrl = builder.url + "/api/ingest";
    this.key = builder.key;
    this.environment = builder.environment;
    this.appName = builder.appName;
    this.instanceId = builder.instanceId;
    this.version = builder.version;
    this.gzip = builder.gzip;
    this.ping = builder.ping;
    this.timeoutSecs = builder.timeoutSecs;
    this.periodSecs = builder.periodSecs;
    if (!builder.databaseList.isEmpty()) {
      this.databaseList.addAll(builder.databaseList);
    }
    this.collectEbeanMetrics = builder.collectEbeanMetrics;
    this.collectAvajeMetrics = builder.isCollectAvajeMetrics();
    this.timer = new Timer("ebeanInsight", true);
    this.httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(15))
      .build();
  }

  /**
   * Return true if this is actively reporting metrics.
   */
  public boolean isActive() {
    return active;
  }

  InsightClient start() {
    if (!enabled) {
      log.debug("insight not enabled");
      return this;
    }
    if (!ping || ping()) {
      active = true;
      long periodMillis = periodSecs * 1000;
      Date first = new Date(System.currentTimeMillis() + periodMillis);
      timer.schedule(new Task(), first, periodMillis);
      log.info("insight enabled");
    }
    return this;
  }

  private void processBody(String responseBody) {
    // process the response commands
  }

  private class Task extends TimerTask {
    @Override
    public void run() {
      send();
    }
  }

  private void send() {
    try {
      long timeStart = System.nanoTime();
      final String json = buildJsonContent();
      long timeCollect = System.nanoTime();
      post(json);
      if (log.isTraceEnabled()) {
        log.trace("send metrics {}", json);
      }
      long timeFinish = System.nanoTime();
      collectMicros = (timeCollect - timeStart) / 1000;
      reportMicros = (timeFinish - timeCollect) / 1000;
      if (log.isDebugEnabled()) {
        log.debug("metrics collect:{} report:{} length:{} latency:{}", collectMicros, reportMicros, contentLength, latencyMillis);
      }

    } catch (Throwable e) {
      log.warn("Error reporting metrics", e);
    }
  }

  String buildJsonContent() {

    Json json = new Json();

    json.append("{");
    json.keyVal("environment", environment);
    json.keyVal("appName", appName);
    json.keyVal("instanceId", instanceId);
    json.keyVal("version", version);
    json.keyVal("eventTime", System.currentTimeMillis());
    json.keyVal("collect", collectMicros);
    json.keyVal("report", reportMicros);
    json.keyVal("latency", latencyMillis);

    if (collectAvajeMetrics) {
      addAvajeMetrics(json);
    }
    if (collectEbeanMetrics) {
      addDatabaseMetrics(json);
    }
    json.append("}");
    return json.asJson();
  }

  private void addAvajeMetrics(Json json) {
    json.key("metrics");
    json.append("[");
    MetricManager.collectAsJson().write(json.buffer);
    json.append("]");
  }

  private void addDatabaseMetrics(InsightClient.Json json) {
    json.key("dbs");
    json.append("[");
    if (databaseList.isEmpty()) {
      // metrics from the default database
      DB.getDefault().getMetaInfoManager().collectMetricsAsJson().write(json.buffer);
    } else {
      for (int i = 0; i < databaseList.size(); i++) {
        if (i > 0) {
          json.buffer.append(",\n");
        }
        databaseList.get(i).getMetaInfoManager().collectMetricsAsJson().write(json.buffer);
      }
    }
    json.append("]");
  }

  static byte[] gzip(String str) throws IOException {
    ByteArrayOutputStream obj = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(obj);
    gzip.write(str.getBytes(StandardCharsets.UTF_8));
    gzip.close();
    return obj.toByteArray();
  }

  private void post(String json) throws IOException {
    contentLength = json.length();
    byte[] input = gzip ? gzip(json) : json.getBytes(StandardCharsets.UTF_8);
    contentLength = input.length;
    httpPost(input, gzip);
  }

  private void httpPost(byte[] input, boolean gzipped) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
      .timeout(Duration.ofSeconds(timeoutSecs))
      .POST(ofByteArray(input))
      .uri(ingestUri)
      .setHeader("Content-Type", "application/json; utf-8")
      .setHeader("Insight-Key", key);

    if (gzipped) {
      builder.setHeader("Content-Encoding", "gzip");
    }

    final long latencyStart = System.currentTimeMillis();
    httpClient.sendAsync(builder.build(), ofString())
      .thenAccept(res -> {
        latencyMillis = System.currentTimeMillis() - latencyStart;
        final int code = res.statusCode();
        if (code < 300) {
          processBody(res.body());
        } else {
          log.info("Failed to send metrics - response code:{} body:{}", code, res.body());
        }
      });
  }

  /**
   * Return true if the insight host can be accessed.
   */
  boolean ping() {
    try {
      final HttpRequest req = HttpRequest.newBuilder()
        .timeout(Duration.ofSeconds(timeoutSecs))
        .uri(URI.create(pingUrl))
        .setHeader("Insight-Key", key)
        .build();
      final HttpResponse<String> response = httpClient.send(req, ofString());
      return response.statusCode() < 300 && "ok".equals(response.body());
    } catch (Exception e) {
      log.debug("Ping unsuccessful " + e);
      return false;
    }
  }

  public static class Builder {

    private boolean enabled;
    private String url;
    private String key;
    private String environment;
    private String appName;
    private String instanceId;
    private String version;
    private int timeoutSecs;
    private long periodSecs;
    private boolean gzip;
    private boolean ping;
    private boolean collectEbeanMetrics;
    private boolean collectAvajeMetrics;
    private final List<Database> databaseList = new ArrayList<>();

    Builder() {
      this.enabled = Config.getBool("ebean.insight.enabled", true);
      this.key = Config.get("ebean.insight.key", System.getenv("INSIGHT_KEY"));
      this.url = Config.get("ebean.insight.url", "https://ebean.co");
      this.periodSecs = Config.getLong("ebean.insight.periodSecs", 60);
      this.timeoutSecs = Config.getInt("ebean.insight.timeoutSecs", 15);
      this.gzip = Config.getBool("ebean.insight.gzip", true);
      this.ping = Config.getBool("ebean.insight.ping", false);
      this.collectEbeanMetrics = Config.getBool("ebean.insight.collectEbeanMetrics", true);
      this.collectAvajeMetrics = Config.getBool("ebean.insight.collectAvajeMetrics", true);
      this.appName = Config.get("app.name", null);
      this.environment = Config.get("app.environment", null);
      this.instanceId = Config.get("app.instanceId", System.getenv("HOSTNAME"));
      this.version = Config.get("app.version", null);
    }

    /**
     * Set the ebean insight key.
     */
    public Builder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * Set the url to send the metrics to.
     */
    public Builder url(String url) {
      this.url = url;
      return this;
    }

    /**
     * Set the environment this server is running in (dev, test, prod etc).
     */
    public Builder environment(String environment) {
      this.environment = environment;
      return this;
    }

    /**
     * Set the application name.
     */
    public Builder appName(String appName) {
      this.appName = appName;
      return this;
    }

    /**
     * Set whether to use gzip content encoding.
     */
    public Builder gzip(boolean gzip) {
      this.gzip = gzip;
      return this;
    }

    /**
     * Set true to skip ping check on startup.
     */
    public Builder ping(boolean ping) {
      this.ping = ping;
      return this;
    }

    /**
     * Set request timeout in seconds. Default timeout when unset is 15 secs.
     */
    public Builder timeoutSecs(int timeoutSecs) {
      this.timeoutSecs = timeoutSecs;
      return this;
    }

    /**
     * Set the application version.
     */
    public Builder version(String version) {
      this.version = version;
      return this;
    }

    /**
     * Set the pod id that identifies the instance.
     */
    public Builder instanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    /**
     * Set the reporting frequency in seconds. Defaults to 60.
     */
    public Builder periodSecs(int periodSecs) {
      this.periodSecs = periodSecs;
      return this;
    }

    /**
     * Add an explicit database to collect metrics on.
     * <p>
     * If we don't explicitly register a database then by default
     * this collects metrics on the default database.
     * </p>
     */
    public Builder database(Database database) {
      this.databaseList.add(database);
      return this;
    }

    /**
     * Set collection of Ebean ORM metrics. Defaults to true.
     */
    public Builder collectEbeanMetrics(boolean collectEbeanMetrics) {
      this.collectEbeanMetrics = collectEbeanMetrics;
      return this;
    }

    /**
     * Set collection of Avaje metrics. Defaults to detect if avaje metrics is present.
     */
    public Builder collectAvajeMetrics(boolean collectAvajeMetrics) {
      this.collectAvajeMetrics = collectAvajeMetrics;
      return this;
    }

    /**
     * Set if metrics collection is enabled or not.
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Not enabled if no valid key provided or explicitly disabled via property.
     */
    boolean enabled() {
      return enabled && validKey();
    }

    private boolean validKey() {
      return key != null && key.trim().length() > 0 && !"none".equalsIgnoreCase(key);
    }

    /**
     * Build the client.
     * <p>
     * We don't need to do anything with the client. It will periodically collect
     * and report metrics using a timer.
     * </p>
     */
    public InsightClient build() {
      return new InsightClient(this).start();
    }

    boolean isCollectAvajeMetrics() {
      return collectAvajeMetrics && detectAvajeMetrics();
    }

    private boolean detectAvajeMetrics() {
      try {
        Class.forName("io.avaje.metrics.MetricManager");
        return true;
      } catch (ClassNotFoundException e) {
        return false;
      }
    }
  }

  /**
   * Internal helper to write json content.
   */
  private static class Json {

    private boolean keyPrefix;

    private final StringBuilder buffer;

    private Json() {
      this.buffer = new StringBuilder(1000);
    }

    void key(String key) {
      preKey();
      str(key);
      buffer.append(":");
    }

    void keyVal(String key, String val) {
      if (val != null) {
        preKey();
        str(key);
        buffer.append(":");
        str(val);
      }
    }

    void keyVal(String key, long val) {
      preKey();
      str(key);
      buffer.append(":");
      buffer.append(val);
    }

    private void preKey() {
      if (keyPrefix) {
        buffer.append(" ,");
      } else {
        keyPrefix = true;
      }
    }

    private void str(String key) {
      buffer.append("\"").append(key).append("\"");
    }

    void append(String raw) {
      buffer.append(raw);
    }

    String asJson() {
      return buffer.toString();
    }
  }
}
