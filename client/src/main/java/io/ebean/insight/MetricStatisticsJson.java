package io.ebean.insight;

import io.avaje.metrics.Counter;
import io.avaje.metrics.GaugeDouble;
import io.avaje.metrics.GaugeLong;
import io.avaje.metrics.Meter;
import io.avaje.metrics.Metric;
import io.avaje.metrics.Timer;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Writes already-collected Avaje metric statistics in the same format as
 * {@code MetricRegistry.collectAsJson()}.
 */
final class MetricStatisticsJson {

  private MetricStatisticsJson() {
  }

  static void write(StringBuilder buffer, List<Metric.Statistics> metrics, boolean v2) {
    for (int i = 0; i < metrics.size(); i++) {
      if (i > 0) {
        buffer.append(',');
      }
      writeMetric(buffer, metrics.get(i), v2);
    }
  }

  private static void writeMetric(StringBuilder buffer, Metric.Statistics metric, boolean v2) {
    buffer.append('{');
    field(buffer, "name", metric.name());
    metric.visit(new Metric.Visitor() {
      @Override
      public void visit(Timer.Stats value) {
        summary(buffer, value);
      }

      @Override
      public void visit(Meter.Stats value) {
        summary(buffer, value);
      }

      @Override
      public void visit(Counter.Stats value) {
        number(buffer, "value", value.count());
      }

      @Override
      public void visit(GaugeDouble.Stats value) {
        number(buffer, "value", format(value.value()));
      }

      @Override
      public void visit(GaugeLong.Stats value) {
        number(buffer, "value", value.value());
      }
    });
    appendTags(buffer, metric.id(), v2);
    buffer.append('}');
  }

  private static void summary(StringBuilder buffer, Meter.Stats value) {
    number(buffer, "count", value.count());
    if (value.count() != 0) {
      number(buffer, "mean", value.mean());
      number(buffer, "max", value.max());
      number(buffer, "total", value.total());
    }
  }

  private static void appendTags(StringBuilder buffer, Metric.ID id, boolean v2) {
    var tags = id.tags();
    if (tags.isEmpty()) {
      return;
    }
    buffer.append(',');
    if (v2) {
      var values = tags.array().clone();
      Arrays.sort(values);
      field(buffer, "tags", String.join(",", values));
    } else {
      buffer.append("\"tags\":[");
      var values = tags.array();
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          buffer.append(',');
        }
        quoted(buffer, values[i]);
      }
      buffer.append(']');
    }
  }

  private static void field(StringBuilder buffer, String name, String value) {
    buffer.append('"').append(name).append("\":");
    quoted(buffer, value);
  }

  private static void number(StringBuilder buffer, String name, long value) {
    buffer.append(',').append('"').append(name).append("\":").append(value);
  }

  private static void number(StringBuilder buffer, String name, String value) {
    buffer.append(',').append('"').append(name).append("\":").append(value);
  }

  private static void quoted(StringBuilder buffer, String value) {
    buffer.append('"').append(JsonEscape.escape(value)).append('"');
  }

  private static String format(double value) {
    var format = new DecimalFormat("0.0#", DecimalFormatSymbols.getInstance(Locale.ROOT));
    return format.format(value);
  }
}
