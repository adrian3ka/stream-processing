package beam.example.trigger;

import beam.example.basic.PubsubToBigQuery;
import beam.example.pubsub.PubsubUtil;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TriggerExampleSeriesOne {
  private static final Class RUNNER = DataflowRunner.class;
  private static final String SERIES = "trigger-example-series-one";
  private static final Integer WINDOW_DURATION = 5;
  private static final Duration ALLOWED_LATENESS = Duration.standardMinutes(5);
  private static final Duration TRIGGER_EVERY = Duration.standardMinutes(1);
  private static final Duration TRIGGER_EVERY_AFTER_LATENESS = Duration.standardMinutes(2);
  static final String TIMESTAMP_KEY = "timestamp_ms";
  static final String DATASET = "cookbook";

  private static final ProjectTopicName TOPIC_NAME_SERIES_ONE =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

  static final Long SECONDS = 1000L;
  static final Long MINUTES = 60 * SECONDS;
  static final Long HOUR = 60 * MINUTES;

  @Getter
  static class InputMessage {
    String key;
    Integer value;
  }

  @AllArgsConstructor
  public static class OutputMessage extends Thread {
    public final String key;
    public final Integer value;
    public final Instant eventTime;
    public final Instant processingTime;

    private ProjectTopicName projectTopicName = TOPIC_NAME_SERIES_ONE;

    OutputMessage(String key, Integer value, Instant time) {
      this.key = key;
      this.value = value;
      this.eventTime = time;
      this.processingTime = time;
    }

    OutputMessage(String key, Integer value, Instant eventTime, Instant processingTime) {
      this.key = key;
      this.value = value;
      this.eventTime = eventTime;
      this.processingTime = processingTime;
    }

    public void start(ProjectTopicName topicName) {
      projectTopicName = topicName;
      this.start();
    }

    @SneakyThrows
    public void run() {
      long delay = processingTime.getMillis() - Instant.now().getMillis();

      JsonObject jsonObject = new JsonObject();

      jsonObject.addProperty("key", key);
      jsonObject.addProperty("value", value);

      Map<String, String> attributes = new HashMap<>();

      attributes.put(TIMESTAMP_KEY, String.valueOf(eventTime.getMillis()));

      try {
        if (delay > 0) {
          System.out.println("Will publishing data " + jsonObject + " to: " + projectTopicName
            + ", sleep until " + processingTime.toString());
          Thread.sleep(delay);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      long lateness = processingTime.getMillis() - eventTime.getMillis();

      System.out.println(String.format(
        "Publishing %s with attributes " + attributes + " event time: %s | processing time: %s with lateness %d",
        jsonObject, eventTime, processingTime, lateness));

      PubsubUtil.publish(
        PubsubUtil.PublishInput.builder()
          .objectToBePublished(jsonObject)
          .projectTopicName(projectTopicName)
          .attributes(attributes)
          .build()
      );
    }
  }

  /**
   * This example will be run at nearest the next 5 minutes window.
   * e.g.
   * 10.00..10.05 pick 10.05
   * 12.15..12.20 pick 12.20
   * 21.00..21.05 pick 21.05
   * <p>
   * Better to run this example not to close to the next window, because the dataflow need to be prepared and initiatted
   * Assuming now is : 10:00:00
   * And the window is 5 one minutes with allowed lateness 5 minutes after.
   * <p>
   * Key (key)     | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 10:06:00  <- late
   * 5             | 20                 | 10:04:10   | 10:05:27  <- late
   * 5             | 60                 | 10:03:59   | 10:04:05
   * <p>
   * 5             | 20                 | 10:03:01   | 10.05:30  <- late
   * 5             | 60                 | 10:01:00   | 10:01:15
   * 5             | 40                 | 10:02:40   | 10:02:43
   * 5             | 60                 | 10:03:20   | 10:07:25  <- late
   * 5             | 60                 | 10:02:00   | 10:03:00
   * <p>
   * 5             | 100                | 10:01:00   | 10:11.01  <- very late data
   * <p>
   * ================================================================================================
   * DEFAULT (NO Allow lateness, Trigger after end of window)
   * ================================================================================================
   * <p>
   * Key (key)     | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 300                | 6                 | true    | true   | ON_TIME
   * <p>
   * ================================================================================================
   * WITH ALLOWED LATENESS (5 Minutes Lateness, Trigger Forever after end of window, Discarding Pane)
   * ================================================================================================
   * Key (key)     | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 300                | 6                 | true    | false  | ON_TIME
   * 5             | 20                 | 1                 | false   | false  | LATE
   * 5             | 20                 | 1                 | false   | false  | LATE
   * 5             | 30                 | 1                 | false   | false  | LATE
   * 5             | 60                 | 1                 | false   | false  | LATE
   * =================================================================================================
   * SPECULATIVE (5 Minutes Lateness, Trigger forever after first element in pane every 1 minute)
   * =================================================================================================
   * Key (key)     | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 50                 | 1                 | true    | false  | EARLY
   * 5             | 140                | 3                 | false   | false  | EARLY
   * 5             | 180                | 4                 | false   | false  | EARLY
   * 5             | 240                | 5                 | false   | false  | EARLY
   * 5             | 300                | 6                 | false   | false  | EARLY
   * 5             | 320                | 7                 | false   | false  | LATE
   * 5             | 340                | 8                 | false   | false  | LATE
   * 5             | 370                | 9                 | false   | false  | LATE
   * 5             | 430                | 10                | false   | false  | LATE
   * =================================================================================================
   * SEQUENTIAL (5 Minutes Lateness, Trigger forever after first element in pane every 1 minute,
   * for late data firing every 2 minutes)
   * =================================================================================================
   * Key (key)     | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 50                 | 1                 | true    | false  | EARLY
   * 5             | 140                | 3                 | false   | false  | EARLY
   * 5             | 180                | 4                 | false   | false  | EARLY
   * 5             | 240                | 5                 | false   | false  | EARLY
   * 5             | 300                | 6                 | false   | false  | ON_TIME
   * 5             | 370                | 9                 | false   | false  | LATE
   * 5             | 430                | 10                | false   | false  | LATE
   * <p>
   * Note : Please run this example using google dataflow because the SDK have some issue that the watermark didn't
   * advancing for pubsub on direct runner. And also please recalculate based on the processing time on the
   * EmitAndAddTimestamp flow. And please don't forget to view the processing time one the record because the
   * pipeline didn't always cut the watermark pane at exactly window time.
   * E.g.
   * The window is 5 minutes, sometimes the watermark could be cut out at 5 minutes 19 seconds. So don't be confused if
   * the data resulted in BQ is different from the example, just recalculate that the timing before 5 minutes 19 seconds
   * counted as EARLY and ON_TIME (before the window closed) data
   */

  static final Instant nextWindow = TriggerCollection.getNearestWindowOf(WINDOW_DURATION);

  static final List<OutputMessage> outputMessages =
    Arrays.asList(
      new OutputMessage("5", 50,
        nextWindow.plus(3 * SECONDS),
        nextWindow.plus(47 * SECONDS)),
      new OutputMessage("5", 30,
        nextWindow.plus(MINUTES),
        nextWindow.plus(MINUTES + 3 * SECONDS)),
      new OutputMessage("5", 30,
        nextWindow.plus(2 * MINUTES),
        nextWindow.plus(6 * MINUTES)),
      new OutputMessage("5", 20,
        nextWindow.plus(4 * MINUTES + 10 * SECONDS),
        nextWindow.plus(5 * MINUTES + 27 * SECONDS)),
      new OutputMessage("5", 60,
        nextWindow.plus(3 * MINUTES + 59 * SECONDS),
        nextWindow.plus(4 * MINUTES + 5 * SECONDS)),
      //================================================5 DATA=========================================================
      new OutputMessage("5", 20,
        nextWindow.plus(3 * MINUTES + SECONDS),
        nextWindow.plus(5 * MINUTES + 30 * SECONDS)),
      new OutputMessage("5", 60,
        nextWindow.plus(MINUTES),
        nextWindow.plus(MINUTES + 15 * SECONDS)),
      new OutputMessage("5", 40,
        nextWindow.plus(2 * MINUTES + 40 * SECONDS),
        nextWindow.plus(2 * MINUTES + 43 * SECONDS)),
      new OutputMessage("5", 60,
        nextWindow.plus(3 * MINUTES + 20 * SECONDS),
        nextWindow.plus(7 * MINUTES + 25 * SECONDS)),
      new OutputMessage("5", 60,
        nextWindow.plus(2 * MINUTES),
        nextWindow.plus(3 * MINUTES)),
      //================================================10 DATA========================================================
      new OutputMessage("5", 100,
        nextWindow.plus(MINUTES),
        nextWindow.plus(11 * MINUTES))
    );

  // The noise message inside the window
  static final List<OutputMessage> randomOutputMessageInsideWindow = Arrays.asList(
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(MINUTES * 30 + SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(2 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 40 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 40 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 40 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(3 * MINUTES + 45 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 30 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS)),
    new OutputMessage("6", 10, nextWindow.plus(4 * MINUTES * 59 * SECONDS))
  );

  // The messages outside the window, this noise is for advancing the watermark at the current pane
  static final List<OutputMessage> randomOutputMessage = Arrays.asList(
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 3 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 6 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 8 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 14 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 24 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 34 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 45 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 52 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 52 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(5 * MINUTES + 56 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 14 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 24 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 34 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 35 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 44 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 52 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 52 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(6 * MINUTES + 56 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 2 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 12 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 22 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 29 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 35 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 44 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 54 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(7 * MINUTES + 56 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 4 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 14 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 24 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 34 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 44 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(8 * MINUTES + 54 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 22 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 27 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 29 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 32 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 36 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 46 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 46 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(9 * MINUTES + 54 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 4 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 6 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 8 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 24 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 26 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 27 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 28 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 29 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 32 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 33 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 34 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 36 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 39 * SECONDS)),
    new OutputMessage("5", 10, nextWindow.plus(10 * MINUTES + 43 * SECONDS))
  );


  /**
   * Add more noise, you can try to remove this and view the difference between them, the watermark never advance so
   * the default and allowedLateness could get all of the data. And the other method (speculative and sequential)
   * will showing data as EARLIER.
   */

  public static void main(String[] args) {
    outputMessages.forEach(Thread::start);

    randomOutputMessage.forEach(Thread::start);
    randomOutputMessageInsideWindow.forEach(Thread::start);

    System.out.println("The example will start ingesting the data at " + nextWindow.toString());

    PubsubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).withValidation().as(PubsubToBigQuery.Options.class);

    options.setRunner(RUNNER);
    options.setJobName(SERIES + "-" + Instant.now().getMillis());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<PubsubMessage> messages = pipeline.apply(
      "ReadPubSubSubscription",
      PubsubIO.readMessages()
        .withTimestampAttribute(TIMESTAMP_KEY)
        .fromSubscription(SUBSCRIPTION_NAME.toString()));

    PCollectionList<TableRow> resultList = messages.apply(ParDo.of(new EmitAndShowTimestamp()))
      .apply(new TriggerCollection.CalculateTotalFlowSeriesOne(
        WINDOW_DURATION,
        ALLOWED_LATENESS,
        TRIGGER_EVERY,
        TRIGGER_EVERY_AFTER_LATENESS
      ));

    for (int i = 0; i < resultList.size(); i++) {
      String stepName = "TriggerExampleSeriesOne_" + TriggerCollection.triggerTypes.get(i);

      TableReference tableRef =
        TriggerCollection.getTableReference(
          ExampleUtils.PROJECT_ID,
          DATASET,
          stepName
        );

      System.out.println("Setting up table " + tableRef.toString());
      resultList.get(i).apply(
        stepName,
        BigQueryIO.writeTableRows().to(tableRef).withSchema(TriggerCollection.getSchema())
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      );
    }

    pipeline.run();
  }

  public static class EmitAndShowTimestamp extends DoFn<PubsubMessage, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      String messageJson = new String(message.getPayload(), StandardCharsets.UTF_8);

      boolean noiseData = context.timestamp().isAfter(nextWindow);

      System.out.println("Receiving message: " + messageJson + " event time " + context.timestamp().toString()
        + " | at " + Instant.now() + (noiseData ? "<- noise data" : ""));

      Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
      InputMessage inputMessage = gson.fromJson(messageJson, InputMessage.class);

      KV<String, Integer> kv = KV.of(inputMessage.key, inputMessage.value);
      context.output(kv);
    }
  }
}
