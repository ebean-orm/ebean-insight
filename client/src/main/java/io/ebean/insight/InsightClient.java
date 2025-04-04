package io.ebean.insight;

import io.avaje.applog.AppLog;
import io.avaje.config.Config;
import io.avaje.metrics.Metrics;
import io.ebean.Database;
import io.ebean.meta.MetaQueryPlan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static java.lang.System.Logger.Level.*;
import static java.net.http.HttpRequest.BodyPublishers.ofByteArray;
import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Client that collects Ebean metrics and Avaje metrics for
 * sending to the monitoring service.
 *
 * <pre>{@code
 *
 *   final InsightClient client = InsightClient.builder()
 *       .url("http://my-ebean-insight-host")
 *       .appName("myapp")
 *       .environment("dev")
 *       .database(myDatabase)
 *       .key("YeahNah")
 *       .build();
 *
 * }</pre>
 */
public class InsightClient {

  static final System.Logger log = AppLog.getLogger("io.ebean.Insight");

  private final boolean enabled;
  private final String key;
  private final String environment;
  private final String appName;
  private final String instanceId;
  private final String version;
  private final URI ingestUri;
  private final URI ingestPlansUri;
  private final String pingUrl;
  private final long periodSecs;
  private final boolean gzip;
  private final boolean collectEbeanMetrics;
  private final boolean collectAvajeMetrics;
  private final List<Database> databaseList = new ArrayList<>();
  private final Timer timer;
  private final HttpClient httpClient;
  private final QueryPlanCapture planCapture;
  private final int timeoutSecs;
  private final boolean ping;
  private boolean active;

  private long latencyMillis;
  private long collectMicros;
  private long reportMicros;

  /**
   * Create a new builder for InsightClient.
   *
   * <pre>{@code
   *
   *   final InsightClient client = InsightClient.builder()
   *       .url("http://my-ebean-insight-host")
   *       .appName("myapp")
   *       .environment("dev")
   *       .database(myDatabase)
   *       .key("YeahNah")
   *       .build();
   *
   * }</pre>
   */
  public static InsightClient.Builder builder() {
    return new InsightClient.Builder();
  }

  private InsightClient(Builder builder) {
    this.enabled = builder.enabled();
    this.ingestUri = URI.create(builder.url + "/api/ingest/metrics");
    this.ingestPlansUri = URI.create(builder.url + "/api/ingest/plans");
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

    if (builder.capturePlans() && !databaseList.isEmpty()) {
      planCapture = new QueryPlanCapture(databaseList.get(0), this, 10);
    } else {
      planCapture = null;
    }
  }

  /**
   * Return true if this is actively reporting metrics.
   */
  public boolean isActive() {
    return active;
  }

  InsightClient start() {
    if (!enabled) {
      log.log(DEBUG, "insight not enabled");
      return this;
    }
    if (!ping || ping()) {
      active = true;
      long periodMillis = periodSecs * 1000;
      Date first = new Date(System.currentTimeMillis() + periodMillis);
      timer.schedule(new Task(), first, periodMillis);
      if (planCapture != null) {
        planCapture.start();
      }
      log.log(INFO, "insight enabled");
    }
    return this;
  }

  private void processBody(String responseBody) {
    if (planCapture != null) {
      if (responseBody != null && !responseBody.isEmpty()) {
        planCapture.process(responseBody);
      }
    }
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
      long contentLength = post(ingestUri, json);
      if (log.isLoggable(TRACE)) {
        log.log(TRACE, "send metrics {0}", json);
      }
      long timeFinish = System.nanoTime();
      collectMicros = (timeCollect - timeStart) / 1000;
      reportMicros = (timeFinish - timeCollect) / 1000;
      if (log.isLoggable(DEBUG)) {
        log.log(DEBUG, "metrics collect:{0} report:{1} length:{2} latency:{3}", collectMicros, reportMicros, contentLength, latencyMillis);
      }

    } catch (Throwable e) {
      log.log(WARNING, "Error reporting metrics", e);
    }
  }

  void sendPlans(List<MetaQueryPlan> plans) {
    try {
      post(ingestPlansUri, buildPlansJson(plans));
    } catch (Throwable e) {
      log.log(WARNING, "Error reporting query plans", e);
    }
  }

  String buildPlansJson(List<MetaQueryPlan> plans) {
    JsonSimple json = new JsonSimple();
    json.begin('{');
    json.keyVal("environment", environment);
    json.keyVal("appName", appName);
    json.key("plans");
    json.begin('[');
    for (int i = 0; i < plans.size(); i++) {
      MetaQueryPlan metaQueryPlan = plans.get(i);
      if (i > 0) {
        json.append(',');
      }
      json.begin('{');
      json.keyVal("hash", metaQueryPlan.hash());
      json.keyVal("whenCaptured", metaQueryPlan.whenCaptured().toString());
      json.keyVal("label", metaQueryPlan.label());
      json.keyVal("queryTimeMicros",metaQueryPlan.queryTimeMicros());
      json.keyVal("captureMicros", metaQueryPlan.captureMicros());
      json.keyVal("captureCount", metaQueryPlan.captureCount());
      json.keyValEscape("bind", metaQueryPlan.bind());
      json.keyValEscape("plan", metaQueryPlan.plan());
      json.keyValEscape("sql", metaQueryPlan.sql());
      json.end('}');
    }
    json.end(']');
    json.end('}');
    return json.toString();
  }

  String buildJsonContent() {
    JsonSimple json = new JsonSimple();

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

  private void addAvajeMetrics(JsonSimple json) {
    json.key("metrics");
    json.append("[");
    Metrics.collectAsJson().write(json.buffer());
    json.append("]");
  }

  private void addDatabaseMetrics(JsonSimple json) {
    if (databaseList.isEmpty()) {
      return;
    }
    json.key("dbs");
    json.append('[');
    for (int i = 0; i < databaseList.size(); i++) {
      if (i > 0) {
        json.buffer().append(',');
      }
      databaseList.get(i).metaInfo().collectMetrics().asJson().write(json.buffer());
    }
    json.append(']');
  }

  static byte[] gzip(String str) throws IOException {
    ByteArrayOutputStream obj = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(obj);
    gzip.write(str.getBytes(StandardCharsets.UTF_8));
    gzip.close();
    return obj.toByteArray();
  }

  private long post(URI uri, String json) throws IOException {
    byte[] input = gzip ? gzip(json) : json.getBytes(StandardCharsets.UTF_8);
    httpPost(uri, input, gzip);
    return input.length;
  }

  private void httpPost(URI uri, byte[] input, boolean gzipped) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
      .timeout(Duration.ofSeconds(timeoutSecs))
      .POST(ofByteArray(input))
      .uri(uri)
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
          log.log(INFO, "Failed to send metrics - response code:{0} body:{1}", code, res.body());
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
      log.log(DEBUG, "Ping unsuccessful {0}", e.toString());
      return false;
    }
  }

  public static class Builder {

    private boolean enabled;
    private boolean capturePlans;
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
      this.key = Config.get("ebean.insight.key", "hi");
      this.url = Config.get("ebean.insight.url", "https://ebean.co");
      this.periodSecs = Config.getLong("ebean.insight.periodSecs", 60);
      this.timeoutSecs = Config.getInt("ebean.insight.timeoutSecs", 15);
      this.gzip = Config.getBool("ebean.insight.gzip", true);
      this.ping = Config.getBool("ebean.insight.ping", false);
      this.collectEbeanMetrics = Config.getBool("ebean.insight.collectEbeanMetrics", true);
      this.collectAvajeMetrics = Config.getBool("ebean.insight.collectAvajeMetrics", true);
      this.appName = Config.getNullable("app.name");
      this.environment = Config.getNullable("app.environment");
      this.instanceId = Config.getNullable("app.instanceId", System.getenv("HOSTNAME"));
      this.version = Config.getNullable("app.version");
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
     * Set if query plan capture is enabled.
     */
    public Builder capturePlans(boolean capturePlans) {
      this.capturePlans = capturePlans;
      return this;
    }

    /**
     * Not enabled if no valid key provided or explicitly disabled via property.
     */
    boolean enabled() {
      return enabled && validKey();
    }

    boolean capturePlans() {
      return capturePlans;
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

}
