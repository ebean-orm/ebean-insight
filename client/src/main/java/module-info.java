module io.ebean.insight {

  exports io.ebean.insight;

  requires transitive java.net.http;
  requires static io.ebean.api;
  requires static io.avaje.metrics;
}
