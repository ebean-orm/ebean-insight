module io.ebean.insight {

  exports io.ebean.insight;

  requires transitive java.net.http;
  requires transitive io.avaje.applog;

  requires static io.ebean.api;
  requires static io.avaje.metrics;
}
