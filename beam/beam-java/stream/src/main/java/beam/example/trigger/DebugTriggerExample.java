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
package beam.example.trigger;

import beam.example.trigger.common.ExampleBigQueryTableOptions;
import beam.example.trigger.common.ExampleOptions;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import beam.example.util.ResourceUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This example illustrates the basic concepts behind triggering. It shows how to use different
 * trigger definitions to produce partial (speculative) results before all the data is processed and
 * to control when updated results are produced for late data. The example performs a streaming
 * analysis of the data coming in from a text file and writes the results to BigQuery. It divides
 * the data into {@link Window windows} to be processed, and demonstrates using various kinds of
 * {@link org.apache.beam.sdk.transforms.windowing.Trigger triggers} to control when the results for
 * each window are emitted.
 *
 * <p>This example uses a portion of real traffic data from San Diego freeways. It contains readings
 * from sensor stations set up along each freeway. Each sensor reading includes a calculation of the
 * 'total flow' across all lanes in that freeway direction.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. The default triggering behavior
 *   2. Late data with the default trigger
 *   3. How to get speculative estimates
 *   4. Combining late data and speculative estimates
 * </pre>
 *
 * <p>Before running this example, it will be useful to familiarize yourself with Beam triggers and
 * understand the concept of 'late data', See: <a
 * href="https://beam.apache.org/documentation/programming-guide/#triggers">
 * https://beam.apache.org/documentation/programming-guide/#triggers</a>
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@code
 * --bigQueryDataset}, and {@code --bigQueryTable} options. If the BigQuery table do not exist, the
 * example will try to create them.
 *
 * <p>The pipeline outputs its results to a BigQuery table. Here are some queries you can use to see
 * interesting results: Replace {@code <enter_table_name>} in the query below with the name of the
 * BigQuery table. Replace {@code <enter_window_interval>} in the query below with the window
 * interval.
 *
 * <p>To see the results of the default trigger, Note: When you start up your pipeline, you'll
 * initially see results from 'late' data. Wait after the window duration, until the first pane of
 * non-late data has been emitted, to see more interesting results. {@code SELECT * FROM
 * enter_table_name WHERE trigger_type = "default" ORDER BY window DESC}
 *
 * <p>To see the late data i.e. dropped by the default trigger, {@code SELECT * FROM
 * <enter_table_name> WHERE trigger_type = "withAllowedLateness" and (timing = "LATE" or timing =
 * "ON_TIME") and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p>To see the the difference between accumulation mode and discarding mode, {@code SELECT * FROM
 * <enter_table_name> WHERE (timing = "LATE" or timing = "ON_TIME") AND (trigger_type =
 * "withAllowedLateness" or trigger_type = "sequential") and freeway = "5" ORDER BY window DESC,
 * processing_time}
 *
 * <p>To see speculative results every minute, {@code SELECT * FROM <enter_table_name> WHERE
 * trigger_type = "speculative" and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p>To see speculative results every five minutes after the end of the window {@code SELECT * FROM
 * <enter_table_name> WHERE trigger_type = "sequential" and timing != "EARLY" and freeway = "5"
 * ORDER BY window DESC, processing_time}
 *
 * <p>To see the first and the last pane for a freeway in a window for all the trigger types, {@code
 * SELECT * FROM <enter_table_name> WHERE (isFirst = true or isLast = true) ORDER BY window}
 *
 * <p>To reduce the number of results for each query we can add additional where clauses. For
 * examples, To see the results of the default trigger, {@code SELECT * FROM <enter_table_name>
 * WHERE trigger_type = "default" AND freeway = "5" AND window = "<enter_window_interval>"}
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class DebugTriggerExample {
  // Numeric value of fixed window duration, in minutes
  private static final int WINDOW_DURATION = 5;
  private static final Duration ALLOWED_LATENESS = Duration.standardDays(1);
  private static final Duration TRIGGER_EVERY = Duration.standardMinutes(1);
  private static final Duration TRIGGER_EVERY_AFTER_LATENESS = Duration.standardMinutes(3);
  // Constants used in triggers.
  // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
  // ONE_MINUTE is used only with processing time before the end of the window
  public static final Duration ONE_MINUTE = Duration.standardMinutes(1);
  // FIVE_MINUTES is used only with processing time after the end of the window
  public static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
  public static final Duration ONE_DAY = Duration.standardDays(1);

  private static final String DATA_SOURCE_PATH = ResourceUtils.getData("mini-trigger-example.csv");

  public static final List<String> triggerTypes = Arrays.asList("default", "withAllowedLateness", "speculative", "sequential");

  /**
   * Extract the freeway and total flow in a reading. Freeway is used as key since we are
   * calculating the total flow for each freeway.
   * <p>
   * This flow should be printing "-> ExtractFlowInfo, *" based on the row on mini-trigger-example.csv data
   */
  static class ExtractFlowInfo extends DoFn<String, KV<String, Integer>> {
    private static final int VALID_NUM_FIELDS = 50;

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] laneInfo = c.element().split(",", -1);
      if ("timestamp".equalsIgnoreCase(laneInfo[0])) {
        // Header row
        // System.out.println("-> ExtractFlowInfo, skip header");
        return;
      }
      if (laneInfo.length < VALID_NUM_FIELDS) {
        // Skip the invalid input.
        // System.out.println("-> ExtractFlowInfo, invalid input, lane info length: " + laneInfo.length);
        return;
      }
      String freeway = laneInfo[2];
      Integer totalFlow = tryIntegerParse(laneInfo[7]);
      // Ignore the records with total flow 0 to easily understand the working of triggers.
      // Skip the records with total flow -1 since they are invalid input.
      if (totalFlow == null || totalFlow <= 0) {
        // System.out.println("-> ExtractFlowInfo, skipping data with total flow: " + totalFlow);
        return;
      }

      KV<String, Integer> kv = KV.of(freeway, totalFlow);

      System.out.println("-> ExtractFlowInfo, emitting data " + kv + " with timestamp : " + c.timestamp());
      c.output(kv);
    }
  }

  /**
   * Inherits standard configuration options.
   */
  public interface TrafficFlowOptions
    extends ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions {

    @Description("Input file to read from")
    @Default.String("stream/src/resources/data/mini-trigger-example.csv")
    String getInput();

    void setInput(String value);

    @Description("Numeric value of window duration for fixed windows, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();

    void setWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Starting TriggerExample......");
    TrafficFlowOptions options =
      PipelineOptionsFactory.fromArgs(ExampleUtils.appendArgs(args)).withValidation().as(TrafficFlowOptions.class);
    options.setStreaming(true);

    options.setBigQuerySchema(TriggerCollection.getSchema());
    options.setTempLocation(ExampleUtils.GCS_TMP_LOCATION);
    options.setRunner(DataflowRunner.class);

    ExampleUtils exampleUtils = new ExampleUtils(options);

    System.out.println("Setting up pipeline......");
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    System.out.println("Reading data from : " + DATA_SOURCE_PATH + "....");

    PCollectionList<TableRow> resultList =
      pipeline
        .apply("ReadMyFile", TextIO.read().from(DATA_SOURCE_PATH))
        .apply("InsertRandomDelays", ParDo.of(new InsertDelays()))
        .apply(ParDo.of(new ExtractFlowInfo()))
        .apply(new TriggerCollection.CalculateTotalFlow(
          WINDOW_DURATION,
          ALLOWED_LATENESS,
          TRIGGER_EVERY,
          TRIGGER_EVERY_AFTER_LATENESS
        ));

    System.out.println("Preparing pipeline for result list size " + resultList.size());

    for (int i = 0; i < resultList.size(); i++) {
      TableReference tableRef =
        TriggerCollection.getTableReference(
          options.getProject(), options.getBigQueryDataset(), "DebugTriggerExample_" + triggerTypes.get(i));

      System.out.println("Setting up table " + tableRef.toString());
      resultList.get(i).apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(TriggerCollection.getSchema())
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }

    PipelineResult result = pipeline.run();

    // ExampleUtils will try to cancel the pipeline and the injector before the program exits.
    exampleUtils.waitToFinish(result);
  }

  /**
   * Add current time to each record. Also insert a delay at random to demo the triggers.
   */
  public static class InsertDelays extends DoFn<String, String> {
    private static final double THRESHOLD = 0.4;
    // MIN_DELAY and MAX_DELAY in minutes.
    private static final int MIN_DELAY = 30;
    private static final int MAX_DELAY = 300;

    @ProcessElement
    public void processElement(ProcessContext c) {
      Instant timestamp = Instant.now();
      Random random = new Random();

      double nextDouble = random.nextDouble();
      boolean validForRandom = nextDouble < THRESHOLD;

      if (validForRandom) {
        int range = MAX_DELAY - MIN_DELAY;
        int delayInMinutes = random.nextInt(range) + MIN_DELAY;

        System.out.println();
        long delayInMillis = TimeUnit.MINUTES.toMillis(delayInMinutes);
        timestamp = new Instant(timestamp.getMillis() - delayInMillis);
      }

      System.out.println("-> InsertDelays, emitting " + c.element() + " | with lateness (" + validForRandom + "):" + timestamp);

      c.outputWithTimestamp(c.element(), timestamp);
    }
  }


  private static Integer tryIntegerParse(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
