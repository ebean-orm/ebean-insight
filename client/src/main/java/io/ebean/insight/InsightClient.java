package io.ebean.insight;

import io.avaje.metrics.MetricManager;
import io.ebean.DB;
import io.ebean.Database;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Client that collects Ebean metrics and Avaje metrics for
 * sending to the monitoring service.
 */
public class InsightClient {

  private static final Logger log = LoggerFactory.getLogger(InsightClient.class);

  private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private final String key;
  private final String environment;
  private final String appName;
  private final String instanceId;
  private final String version;
  private final String ingestUrl;
  private final long periodSecs;
  private final boolean collectEbeanMetrics;
  private final boolean collectAvajeMetrics;
  private final List<Database> databaseList = new ArrayList<>();

  private final OkHttpClient client;

  private final Timer timer;

  public static InsightClient.Builder create() {
    return new InsightClient.Builder();
  }

  private InsightClient(Builder builder) {
    this.ingestUrl = builder.url + "/api/ingest/metrics";
    this.key = builder.key;
    this.environment = builder.environment;
    this.appName = builder.appName;
    this.instanceId = builder.instanceId;
    this.version = builder.version;
    this.periodSecs = builder.periodSecs;
    if (!builder.databaseList.isEmpty()) {
      this.databaseList.addAll(builder.databaseList);
    }
    this.collectEbeanMetrics = builder.collectEbeanMetrics;
    this.collectAvajeMetrics = builder.isCollectAvajeMetrics();
    this.client = builder.getHttpClient();
    this.timer = new Timer("MonitorSend", true);
  }

  InsightClient start() {
    long periodMillis = periodSecs * 1000;
    Date first = new Date(System.currentTimeMillis() + periodMillis);
    timer.schedule(new Task(), first, periodMillis);
    return this;
  }

  private class Task extends TimerTask {
    @Override
    public void run() {
      send();
    }
  }

  private void send() {
    try {
      final String json = buildJsonContent();
      log.debug("send metrics {}", json);
      final String responseBody = post(ingestUrl, json);
      log.debug("metrics response {}", responseBody);
    } catch (Throwable e) {
      log.error("Error reporting metrics", e);
    }
  }

  String buildJsonContent() {

    Json json = new Json();

    json.append("{");
    json.keyVal("environment", environment);
    json.keyVal("appName", appName);
    json.keyVal("instanceId", instanceId);
    json.keyVal("version", version);
    json.key("eventTime");
    json.append(System.currentTimeMillis());
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

  private String post(String url, String json) throws IOException {

    final Request.Builder builder = new Request.Builder()
      .url(url)
      .post(RequestBody.create(json, JSON))
      .addHeader("Content-Type", "application/json")
      .addHeader("Insight-Key", key);

    return post(builder.build());
  }

  private String post(Request request) throws IOException {
    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        final ResponseBody body = response.body();
        return body == null ? null : body.string();
      } else {
        throw new IOException("Failed request code:" + response.code() + " body:" + response.body() + " url:" + ingestUrl);
      }
    }
  }

  public static class Builder {

    private String url;
    private String key;
    private String environment;
    private String appName;
    private String instanceId;
    private String version;
    private long periodSecs = 60;
    private boolean collectEbeanMetrics = true;
    private boolean collectAvajeMetrics = true;
    private final OkHttpClient client;
    private final List<Database> databaseList = new ArrayList<>();

    Builder() {
      this.client = new OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .writeTimeout(5, TimeUnit.SECONDS)
        .readTimeout(5, TimeUnit.SECONDS)
        .callTimeout(5, TimeUnit.SECONDS)
        .build();

      initFromSystemProperties();
    }

    private void initFromSystemProperties() {
      final String podName = System.getenv("POD_NAME");
      final String podService = podService(podName);
      this.url = System.getProperty("ebean.insight.url", System.getenv("INSIGHT_URL"));
      this.key = System.getProperty("ebean.insight.key", System.getenv("INSIGHT_KEY"));
      this.appName = System.getProperty("appName", podService);
      this.instanceId = System.getProperty("appInstanceId", podName);
      this.environment = System.getProperty("appEnvironment", System.getenv("POD_NAMESPACE"));
      this.version = System.getProperty("appVersion", System.getenv("POD_VERSION"));
    }

    String podService(String podName) {
      if (podName != null && podName.length() > 16) {
        int p0 = podName.lastIndexOf('-', podName.length() - 16);
        if (p0 > -1) {
          return podName.substring(0, p0);
        }
      }
      return null;
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

    OkHttpClient getHttpClient() {
      return client;
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

    private int keyCount;

    private final StringBuilder buffer;

    private Json() {
      this.buffer = new StringBuilder(1000);
    }

    void key(String key) {

      if (keyCount++ > 0) {
        buffer.append(",");
      }
      str(key);
      buffer.append(":");
    }

    void keyVal(String key, String val) {
      if (val != null) {
        if (keyCount++ > 0) {
          buffer.append("\n,");
        }
        str(key);
        buffer.append(":");
        str(val);
      }
    }

    private void str(String key) {
      buffer.append("\"").append(key).append("\"");
    }

    void append(long currentTimeMillis) {
      buffer.append(currentTimeMillis);
    }

    void append(String raw) {
      buffer.append(raw);
    }

    String asJson() {
      return buffer.toString();
    }
  }
}
