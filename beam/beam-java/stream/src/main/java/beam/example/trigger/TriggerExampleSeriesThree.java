package beam.example.trigger;

import beam.example.basic.PubsubToBigQuery;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.nio.charset.StandardCharsets;

import static beam.example.trigger.TriggerExampleSeriesOne.*;


public class TriggerExampleSeriesThree {
  private static final Class RUNNER = DataflowRunner.class;
  private static final String SERIES = "trigger-example-series-three";

  private static final Duration SESSION_GAP_DURATION = Duration.standardSeconds(30);

  private static final ProjectTopicName TOPIC_NAME_SERIES_THREE =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

  public static class CalculateTotalFlowSeriesThree extends PTransform<PCollection<KV<String, Integer>>, PCollectionList<TableRow>> {
    private Duration gapDuration;

    public CalculateTotalFlowSeriesThree(
      Duration gapDuration
    ) {
      this.gapDuration = gapDuration;
    }

    @Override
    public PCollectionList<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      System.out.println("Window gap duration: " + gapDuration);

      PCollection<TableRow> defaultTriggerResults = flowInfo
        .apply("Default", Window
          .<KV<String, Integer>>into(
            Sessions.withGapDuration(gapDuration))
          .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
          .withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes())
        .apply(new TriggerCollection.TotalFlow("default"));

      return PCollectionList.of(defaultTriggerResults);
    }
  }
  /**
   * ===============================================================================
   * SESSION WINDOW
   * ===============================================================================
   * The data is referred on the previous example.
   * This example using sliding window with
   *
   * <p>
   * Key (key)     | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47  <- late
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 10:06:00  <- late
   * 5             | 20                 | 10:04:10   | 10:05:27  <- late
   * 5             | 60                 | 10:03:59   | 10:04:05
   * <p>
   * 5             | 20                 | 10:03:01   | 10.05:30  <- late
   * 5             | 60                 | 10:01:00   | 10:01:15
   * 5             | 40                 | 10:02:40   | 10:02:43
   * 5             | 60                 | 10:03:20   | 10:07:25  <- late
   * 5             | 60                 | 10:02:00   | 10:03:00  <- late
   * <p>
   * 5             | 100                | 10:01:00   | 10:11.01  <- very late data
   * <p>
   *
   * ===========================================================================================================
   * default (with gap duration 30 seconds)
   * ===========================================================================================================
   * <p>
   * Key (key)     | Value (total_flow) | number_of_records | isFirst | isLast | timing  | window
   * 5             | 90                 | 2                 | true    | true   | ON_TIME | [10:01:00...10:01:30]
   * 5             | 40                 | 1                 | true    | true   | ON_TIME | [10:02:40...10:03:10]
   * 5             | 60                 | 1                 | true    | true   | ON_TIME | [10:03:59...10:04:29]
   * Note : The data calculated as late and didn't go into default mode if the data is more than 30 seconds(session gap)
   * from the event time, and will not included in any window.
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

  /**
   * Add more noise, you can try to remove this and view the difference between them, the watermark never advance so
   * the default and allowedLateness could get all of the data. And the other method (speculative and sequential)
   * will showing data as EARLIER.
   */

  public static void main(String[] args) {
    outputMessages.forEach(outputMessage -> outputMessage.start(TOPIC_NAME_SERIES_THREE));
    randomOutputMessageInsideWindow.forEach(outputMessage -> outputMessage.start(TOPIC_NAME_SERIES_THREE));
    TriggerExampleSeriesTwo.randomOutputMessageDifferentKey
      .forEach(outputMessage -> outputMessage.start(TOPIC_NAME_SERIES_THREE));

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
      .apply(new CalculateTotalFlowSeriesThree(
        SESSION_GAP_DURATION
      ));

    for (int i = 0; i < resultList.size(); i++) {
      String stepName = "TriggerExampleSeriesThree_" + TriggerCollection.triggerTypes.get(i);

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
