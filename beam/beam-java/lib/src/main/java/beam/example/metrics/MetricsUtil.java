package beam.example.metrics;

import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;

public class MetricsUtil {
  public static void displayMetricsData(MetricQueryResults metricQueryResults) {
    for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
      System.out.println(counter.getName() + ":" + counter.getAttempted());
    }

    for (MetricResult<GaugeResult> gauge : metricQueryResults.getGauges()) {
      System.out.println(gauge.getName() + ":" + gauge.getAttempted());
    }

    for (MetricResult<DistributionResult> distribution : metricQueryResults.getDistributions()) {
      System.out.println(distribution.getName() + ":" + distribution.getAttempted());
    }
  }
}
