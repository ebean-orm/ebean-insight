package io.ebean.insight;

import io.avaje.applog.AppLog;
import io.avaje.config.Config;
import io.avaje.metrics.MetricRegistry;
import io.avaje.metrics.Metrics;
import io.avaje.metrics.ebean.DatabaseMetricSupplier;
import io.ebean.Database;
import io.ebean.meta.MetaQueryPlan;
import io.ebean.meta.ServerMetrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;

import static java.lang.System.Logger.Level.*;
import static java.net.http.HttpRequest.BodyPublishers.ofByteArray;
import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Client that forwards Ebean and Avaje metrics to the insight server and
 * captures Ebean query plans.
 *
 * <p>By default InsightClient acts as a <em>forwarder</em>: it does not poll
 * metrics itself ({@code collectEbeanMetrics} and {@code collectAvajeMetrics}
 * both default to false). An upstream collector owns metric collection and feeds
 * snapshots in (see <em>External metric feed</em> below), while this client
 * forwards them on and — when {@code capturePlans(true)} and a database are
 * registered — captures query plans. Opt in to {@code collectEbeanMetrics} /
 * {@code collectAvajeMetrics} to instead make this client the primary collector.
 *
 * <pre>{@code
 *
 *   final InsightClient client = InsightClient.builder()
 *       .url("http://my-ebean-insight-host")
 *       .appName("myapp")
 *       .environment("dev")
 *       .database(myDatabase)
 *       .capturePlans(true)
 *       .key("YeahNah")
 *       .build();
 *
 * }</pre>
 *
 * <h2>External metric feed</h2>
 * InsightClient implements {@link Consumer}{@code <ServerMetrics>} so a single
 * upstream collector (e.g. {@code DatabaseMetricSupplier} in avaje-metrics-ebean)
 * can own the reset-on-read poll of {@code metaInfo()} and fan the snapshot out
 * to this client. Each {@link #accept(ServerMetrics)} call POSTs the snapshot
 * immediately to insight-server.
 * <p>
 * The {@link #register()} convenience wires this up per registered database
 * ({@code build().register()}); for full control build the supplier explicitly
 * with {@code DatabaseMetricSupplier.builder(database).forwardTo(client)}.
 * <p>
 * With this pattern leave {@code collectEbeanMetrics(false)} (the default) so the
 * client's internal Timer doesn't also poll ebean metrics. With both
 * {@code collectEbeanMetrics} and {@code collectAvajeMetrics} false, the metric
 * Timer task is not scheduled at all — only {@link QueryPlanCapture} runs.
 *
 * <h2>Lambda / synchronous mode</h2>
 * {@code lambdaMode(true)} runs with <em>no background threads</em>: the metric
 * Timer is not scheduled, the {@link QueryPlanCapture} background poll is not
 * started, metric/plan POSTs are synchronous, and query-plan capture is advanced
 * inline from {@link #accept(ServerMetrics)} each report cycle. This suits AWS
 * Lambda (and similar freeze/thaw runtimes) where background timers and async
 * HTTP callbacks are unreliable — an upstream avaje-metrics {@code ScheduledTask}
 * drives {@code accept()} so the Lambda's {@code scheduledTask.waitIfRunning()}
 * drain covers insight reporting too. See {@link Builder#lambdaMode(boolean)}.
 */
public class InsightClient implements Consumer<ServerMetrics> {

  static final System.Logger log = AppLog.getLogger("io.ebean.Insight");

  private final boolean enabled;
  private final String key;
  private final String environment;
  private final String appName;
  private final String instanceId;
  private final String version;
  private final Map<String, String> resAttrs;
  private final URI ingestUri;
  private final URI ingestPlansUri;
  private final String pingUrl;
  private final long periodSecs;
  private final boolean gzip;
  private final boolean collectEbeanMetrics;
  private final boolean collectAvajeMetrics;
  private final boolean lambdaMode;
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
  private long lastEventTime;

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
    this.resAttrs = Map.copyOf(builder.resAttrs);
    this.gzip = builder.gzip;
    this.ping = builder.ping;
    this.timeoutSecs = builder.timeoutSecs;
    this.periodSecs = builder.periodSecs;
    if (!builder.databaseList.isEmpty()) {
      this.databaseList.addAll(builder.databaseList);
    }
    this.collectEbeanMetrics = builder.collectEbeanMetrics;
    this.collectAvajeMetrics = builder.isCollectAvajeMetrics();
    this.lambdaMode = builder.lambdaMode;
    this.timer = new Timer("ebeanInsight", true);
    this.httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(15))
      .build();

    if (builder.capturePlans() && !databaseList.isEmpty()) {
      planCapture = new QueryPlanCapture(databaseList.get(0), this, 10, builder.queryPlanListener());
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

  /**
   * Register a {@link DatabaseMetricSupplier} for each registered database that
   * forwards every reset-on-read snapshot to this client, using the default
   * avaje-metrics registry. Returns this client for fluent use:
   *
   * <pre>{@code
   *   InsightClient.builder()
   *       .appName("myapp")
   *       .environment("dev")
   *       .database(database)
   *       .capturePlans(true)
   *       .build()
   *       .register();
   * }</pre>
   *
   * <p>This is the blessed convenience for the <em>forwarder</em> role: a single
   * avaje-metrics-owned poll collects the Ebean metrics and fans the snapshot out
   * here via {@link #accept(ServerMetrics)}. Leave {@code collectEbeanMetrics}
   * false (the default) so the client's own Timer does not <em>also</em> poll the
   * same database (a double reset-on-read split).
   *
   * <p>Requires {@code avaje-metrics-ebean} on the classpath/module-path (an
   * optional dependency). For full control over supplier options
   * ({@code legacyNames}, pool metrics) build the supplier explicitly with
   * {@code DatabaseMetricSupplier.builder(database).forwardTo(client)} instead.
   *
   * @return this client
   */
  public InsightClient register() {
    if (databaseList.isEmpty()) {
      log.log(WARNING, "InsightClient.register() called with no database registered - nothing to forward");
      return this;
    }
    if (collectEbeanMetrics) {
      log.log(WARNING, "InsightClient.register() with collectEbeanMetrics(true) double-polls the database - prefer the forwarder default collectEbeanMetrics(false)");
    }
    for (Database database : databaseList) {
      DatabaseMetricSupplier.builder(database).forwardTo(this).build().register();
    }
    return this;
  }

  /**
   * Register a {@link DatabaseMetricSupplier} per database to the given
   * {@link MetricRegistry}, forwarding each snapshot to this client.
   * See {@link #register()} for the common (default registry) case.
   *
   * @return this client
   */
  public InsightClient register(MetricRegistry registry) {
    for (Database database : databaseList) {
      DatabaseMetricSupplier.builder(database).forwardTo(this).build().register(registry);
    }
    return this;
  }

  InsightClient start() {
    if (!enabled) {
      log.log(DEBUG, "insight not enabled");
      return this;
    }
    if (!ping || ping()) {
      active = true;
      lastEventTime = System.currentTimeMillis();
      if (!lambdaMode && (collectEbeanMetrics || collectAvajeMetrics)) {
        long periodMillis = periodSecs * 1000;
        Date first = new Date(lastEventTime + periodMillis);
        timer.schedule(new Task(), first, periodMillis);
      }
      if (planCapture != null && !lambdaMode) {
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

  /**
   * Accept an externally-collected {@link ServerMetrics} snapshot and POST it
   * to insight-server immediately. Lets an upstream collector (e.g.
   * {@code DatabaseMetricSupplier}) own the single reset-on-read poll of
   * {@code metaInfo()} and fan the snapshot out to this client.
   * <p>
   * When using this method leave {@code collectEbeanMetrics(false)} so the
   * client's internal Timer doesn't also poll ebean metrics.
   */
  @Override
  public void accept(ServerMetrics metrics) {
    if (!enabled || !active || metrics == null) {
      return;
    }
    try {
      post(ingestUri, buildExternalMetricsJson(metrics));
    } catch (Throwable e) {
      log.log(WARNING, "Error reporting ebean metrics", e);
    }
    if (lambdaMode && planCapture != null) {
      // no background timer in lambdaMode - advance the query-plan capture
      // state machine inline so it runs on (and is awaited by) the caller's
      // thread. The metrics POST above is synchronous in lambdaMode, so its
      // response has already armed any new plans before we progress here.
      planCapture.progress();
    }
  }

  private String buildExternalMetricsJson(ServerMetrics metrics) {
    final long eventTime;
    final long startEventTime;
    synchronized (this) {
      eventTime = System.currentTimeMillis();
      startEventTime = lastEventTime;
      lastEventTime = eventTime;
    }
    JsonSimple json = new JsonSimple();
    json.append("{");
    json.keyVal("environment", environment);
    json.keyVal("appName", appName);
    json.keyVal("instanceId", instanceId);
    json.keyVal("version", version);
    json.keyVal("eventTime", eventTime);
    json.keyVal("startEventTime", startEventTime);
    json.keyValMap("resAttrs", resAttrs);
    json.key("dbs");
    json.append('[');
    metrics.asJson().write(json.buffer());
    json.append(']');
    json.append("}");
    return json.asJson();
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
    final long eventTime = System.currentTimeMillis();
    final long startEventTime = lastEventTime;
    // Advance immediately: collectMetrics(reset=true) below captures the delta
    // since the previous collect regardless of whether this POST succeeds, so
    // the next request's window must start at this eventTime.
    lastEventTime = eventTime;

    JsonSimple json = new JsonSimple();

    json.append("{");
    json.keyVal("environment", environment);
    json.keyVal("appName", appName);
    json.keyVal("instanceId", instanceId);
    json.keyVal("version", version);
    json.keyVal("eventTime", eventTime);
    json.keyVal("startEventTime", startEventTime);
    json.keyVal("collect", collectMicros);
    json.keyVal("report", reportMicros);
    json.keyVal("latency", latencyMillis);
    json.keyValMap("resAttrs", resAttrs);

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
    if (lambdaMode) {
      // synchronous POST so the response (which carries query-plan capture
      // directives) is processed on the caller's thread before returning -
      // no background HttpClient callback that could be suspended by a Lambda
      // freeze.
      try {
        HttpResponse<String> res = httpClient.send(builder.build(), ofString());
        latencyMillis = System.currentTimeMillis() - latencyStart;
        handleResponse(res.statusCode(), res.body());
      } catch (IOException e) {
        log.log(WARNING, "Failed to send metrics - {0}", e.toString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.log(WARNING, "Interrupted sending metrics - {0}", e.toString());
      }
      return;
    }
    httpClient.sendAsync(builder.build(), ofString())
      .whenComplete((res, ex) -> {
        if (ex != null) {
          log.log(WARNING, "Failed to send metrics - {0}", ex.toString());
          return;
        }
        latencyMillis = System.currentTimeMillis() - latencyStart;
        handleResponse(res.statusCode(), res.body());
      });
  }

  private void handleResponse(int code, String body) {
    if (code < 300) {
      processBody(body);
    } else {
      log.log(INFO, "Failed to send metrics - response code:{0} body:{1}", code, body);
    }
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
    private Consumer<MetaQueryPlan> queryPlanListener;
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
    private boolean lambdaMode;
    private final List<Database> databaseList = new ArrayList<>();
    private final Map<String, String> resAttrs = new LinkedHashMap<>();

    Builder() {
      this.enabled = Config.getBool("ebean.insight.enabled", true);
      this.key = Config.get("ebean.insight.key", "hi");
      this.url = Config.get("ebean.insight.url", "https://ebean.co");
      this.periodSecs = Config.getLong("ebean.insight.periodSecs", 60);
      this.timeoutSecs = Config.getInt("ebean.insight.timeoutSecs", 15);
      this.gzip = Config.getBool("ebean.insight.gzip", true);
      this.ping = Config.getBool("ebean.insight.ping", false);
      this.collectEbeanMetrics = Config.getBool("ebean.insight.collectEbeanMetrics", false);
      this.collectAvajeMetrics = Config.getBool("ebean.insight.collectAvajeMetrics", false);
      this.lambdaMode = Config.getBool("ebean.insight.lambdaMode", false);
      this.appName = Config.getNullable("app.name");
      // Primary is the avaje standard 'app.environment'; fall back to the
      // 'app.env' property / APP_ENV env var used by some apps. OTEL resource
      // attributes back-fill later in applyResourceAttributeDefaults().
      this.environment = firstNonBlank(
        Config.getNullable("app.environment"),
        Config.getNullable("app.env", System.getenv("APP_ENV")));
      this.instanceId = Config.getNullable("app.instanceId", System.getenv("HOSTNAME"));
      this.version = Config.getNullable("app.version");
      // Auto-populate resource attributes from the standard OTEL env var
      // (also honour Java system property of the same name).
      String otelAttrs = System.getenv("OTEL_RESOURCE_ATTRIBUTES");
      if (otelAttrs == null || otelAttrs.isBlank()) {
        otelAttrs = System.getProperty("otel.resource.attributes");
      }
      this.resAttrs.putAll(OtelResourceAttributes.parse(otelAttrs));
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
     * <p>
     * When not set explicitly (here or via the {@code app.environment} or
     * {@code app.env} property / {@code APP_ENV} env var) it falls back to the
     * {@code deployment.environment.name} OTEL resource attribute, so OTEL-native
     * deployments report the correct environment.
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
     * Add an additional OTel resource attribute that will be forwarded to
     * any downstream OTLP exporter on the insight-server. By default, attributes
     * are auto-populated from the {@code OTEL_RESOURCE_ATTRIBUTES} env var
     * (using the standard W3C-baggage-style {@code key=value,key=value} syntax).
     * Use this method to add or override entries programmatically.
     *
     * <p>The reserved keys {@code service.name}, {@code service.version},
     * {@code service.instance.id} and {@code deployment.environment.name} are
     * always sourced from the dedicated builder fields and will be ignored
     * here by the server.
     */
    public Builder resourceAttribute(String key, String value) {
      if (key != null && !key.isEmpty() && value != null) {
        this.resAttrs.put(key, value);
      }
      return this;
    }

    /**
     * Add OTel resource attributes from a single {@code key=value,key2=value2}
     * string — the same format as the {@code OTEL_RESOURCE_ATTRIBUTES} env var.
     * Values may be URL-encoded (per W3C baggage). Existing entries with the
     * same key are overwritten.
     *
     * <pre>{@code
     *   .resourceAttributes("business.domain=ingestion,business.system=consolidation")
     * }</pre>
     */
    public Builder resourceAttributes(String rawKeyValues) {
      this.resAttrs.putAll(OtelResourceAttributes.parse(rawKeyValues));
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
     * Set collection of Ebean ORM metrics. Defaults to false.
     * <p>
     * Leave this false (the default) when InsightClient is a forwarder — i.e. an
     * upstream collector (e.g. {@code DatabaseMetricSupplier} in
     * avaje-metrics-ebean) owns the single reset-on-read poll of {@code metaInfo()}
     * and feeds snapshots in via {@link InsightClient#accept(ServerMetrics)}.
     * Setting both this and that external feed active double-polls the
     * reset-on-read metrics and splits the deltas. Set this true only when
     * InsightClient itself is the primary Ebean metrics collector.
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
     * Run with no background threads, performing all I/O synchronously on the
     * calling thread - for AWS Lambda (and similar freeze/thaw runtimes) where
     * background timers and async HTTP callbacks are unreliable.
     * <p>
     * When enabled: the internal metric {@code Timer} is never scheduled and the
     * {@link QueryPlanCapture} background poll is not started; metric/plan POSTs
     * are synchronous (so the response - which carries plan-capture directives -
     * is handled before returning); and query-plan capture is advanced inline
     * from {@link InsightClient#accept(ServerMetrics)} each report cycle.
     * <p>
     * Designed for the forwarder role: an upstream avaje-metrics
     * {@code ScheduledTask} owns the poll and drives {@code accept()}, so the
     * Lambda's existing {@code scheduledTask.waitIfRunning()} drain also covers
     * insight reporting. Defaults to false (config {@code ebean.insight.lambdaMode}).
     */
    public Builder lambdaMode(boolean lambdaMode) {
      this.lambdaMode = lambdaMode;
      return this;
    }

    /**
     * Register a listener notified for each query plan as it is captured.
     * <p>
     * The listener is invoked once per captured {@link MetaQueryPlan} on Ebean's
     * background executor thread, independent of whether the plan is successfully
     * sent to the insight server. This allows the application to log captured plans
     * (for example, as SLF4J structured key/value logs). Any exception thrown by the
     * listener is caught and logged so it cannot disrupt capture or sending.
     *
     * <pre>{@code
     *
     *   InsightClient.builder()
     *     .database(db)
     *     .capturePlans(true)
     *     .onQueryPlanCaptured(plan ->
     *       log.atInfo()
     *         .addKeyValue("ebean.plan.hash", plan.hash())
     *         .addKeyValue("ebean.plan.label", plan.label())
     *         .addKeyValue("ebean.plan.queryTimeMicros", plan.queryTimeMicros())
     *         .log("ebean query plan captured"))
     *     .build();
     *
     * }</pre>
     */
    public Builder onQueryPlanCaptured(Consumer<MetaQueryPlan> listener) {
      this.queryPlanListener = listener;
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

    boolean collectEbeanMetrics() {
      return collectEbeanMetrics;
    }

    boolean lambdaMode() {
      return lambdaMode;
    }

    Consumer<MetaQueryPlan> queryPlanListener() {
      return queryPlanListener;
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
      applyResourceAttributeDefaults();
      return new InsightClient(this).start();
    }

    /**
     * Back-fill the reserved fields from the OTEL resource attributes when they
     * were not set explicitly (via the {@code app.*} properties or builder
     * methods). This lets apps configured the OTEL-native way — supplying only
     * {@code OTEL_RESOURCE_ATTRIBUTES} (e.g. {@code deployment.environment.name},
     * {@code service.name}) — still report the correct environment, service name,
     * version and instance id on both metrics and captured query plans.
     */
    private void applyResourceAttributeDefaults() {
      if (environment == null) {
        environment = firstNonBlank(resAttrs.get("deployment.environment.name"),
          resAttrs.get("deployment.environment"));
      }
      if (appName == null) {
        appName = blankToNull(resAttrs.get("service.name"));
      }
      if (version == null) {
        version = blankToNull(resAttrs.get("service.version"));
      }
      if (instanceId == null) {
        instanceId = blankToNull(resAttrs.get("service.instance.id"));
      }
    }

    private static String firstNonBlank(String a, String b) {
      String v = blankToNull(a);
      return v != null ? v : blankToNull(b);
    }

    private static String blankToNull(String v) {
      return (v == null || v.isBlank()) ? null : v;
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
