/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package beam.example.metrics;

import beam.example.basic.DebuggingWordCount;
import beam.example.basic.WindowedWordCount;
import beam.example.basic.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link WordCountWithMetrics}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class WordCountWithMetrics {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
      .apply(ParDo.of(new MyCounterMetricsDoFn("namespace", "lineCounter")))
      .apply(ParDo.of(new MyGaugeMetricsDoFn("namespace", "lastLineLength")))
      .apply(ParDo.of(new MyDistributionMetricsDoFn("namespace", "lineLengthDistribution")))
      .apply(
        FlatMapElements.into(TypeDescriptors.strings())
          .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
      .apply(Filter.by((String word) -> !word.isEmpty()))
      .apply(ParDo.of(new MyGaugeMetricsDoFn("namespace", "lastWordLength")))
      .apply(ParDo.of(new MyDistributionMetricsDoFn("namespace", "wordLengthDistribution")))

      .apply(Count.perElement())
      .apply(
        MapElements.into(TypeDescriptors.strings())
          .via(
            (KV<String, Long> wordCount) ->
              wordCount.getKey() + ": " + wordCount.getValue()))
      .apply(ParDo.of(new MyCounterMetricsDoFn("namespace", "uniqueWordCounter")))
      .apply(TextIO.write().to("minimal-word-count"));

    PipelineResult pipelineResult = pipeline.run();

    // request the metric called "lineCounter" and "uniqueWordCounter"
    // in namespace called "namespace"
    MetricQueryResults metrics =
      pipelineResult
        .metrics()
        .queryMetrics(
          MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named("namespace", "lineCounter"))
            .addNameFilter(MetricNameFilter.named("namespace", "uniqueWordCounter"))
            .build());

    // print the metric value - there should be only one line because there is only one metric
    // called "counter1" in the namespace called "namespace"
    for (MetricResult<Long> counter : metrics.getCounters()) {
      System.out.println(counter.getName() + ":" + counter.getAttempted());
    }

    // request the metric in namespace called "namespace"
    MetricQueryResults metricsInNamespace =
      pipelineResult
        .metrics()
        .queryMetrics(
          MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace("namespace"))
            .build());

    System.out.println("========================================================");

    // print the metric value - there should be only one line because there is only one metric
    // called "counter1" in the namespace called "namespace"
    for (MetricResult<Long> counter : metricsInNamespace.getCounters()) {
      System.out.println(counter.getName() + ":" + counter.getAttempted());
    }

    for (MetricResult<GaugeResult> gauge : metricsInNamespace.getGauges()) {
      System.out.println(gauge.getName() + ":" + gauge.getAttempted());
    }

    for (MetricResult<DistributionResult> distribution : metricsInNamespace.getDistributions()) {
      System.out.println(distribution.getName() + ":" + distribution.getAttempted());
    }
  }

  public static class MyGaugeMetricsDoFn extends DoFn<String, String> {
    private Gauge gauge;

    public MyGaugeMetricsDoFn(
      String metricsNamespace,
      String metricsName
    ) {
      gauge = Metrics.gauge(metricsNamespace, metricsName);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      int stringLength = context.element().length();
      boolean emptyWord = stringLength == 0;

      if (!emptyWord) {
        gauge.set(stringLength);
      }

      context.output(context.element());
    }
  }

  public static class MyDistributionMetricsDoFn extends DoFn<String, String> {
    private Distribution distribution;

    public MyDistributionMetricsDoFn(
      String metricsNamespace,
      String metricsName
    ) {
      distribution = Metrics.distribution(metricsNamespace, metricsName);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      int stringLength = context.element().length();
      boolean emptyWord = stringLength == 0;

      if (!emptyWord) {
        distribution.update(stringLength);
      }

      context.output(context.element());
    }
  }

  public static class MyCounterMetricsDoFn extends DoFn<String, String> {
    private Counter counter;

    public MyCounterMetricsDoFn(
      String metricsNamespace,
      String metricsName
    ) {
      counter = Metrics.counter(metricsNamespace, metricsName);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      // count the elements
      counter.inc();
      context.output(context.element());
    }
  }
}
