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
import org.joda.time.DateTime;
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
  private static final String TIMESTAMP_KEY = "timestamp_ms";
  private static final String DATASET = "cookbook";

  private static final ProjectTopicName TOPIC_NAME =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

  private static final Long SECONDS = 1000L;
  private static final Long MINUTES = 60 * SECONDS;
  private static final Long HOUR = 60 * MINUTES;

  @Getter
  private static class InputMessage {
    private String key;
    private Integer value;
  }

  @AllArgsConstructor
  private static class OutputMessage extends Thread {
    public final String key;
    public final Integer value;
    public final Instant eventTime;
    public final Instant processingTime;

    @SneakyThrows
    public void run() {
      Long delay = processingTime.getMillis() - Instant.now().getMillis();

      try {
        if (delay > 0) {
          Thread.sleep(delay);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      JsonObject jsonObject = new JsonObject();

      jsonObject.addProperty("key", key);
      jsonObject.addProperty("value", value);

      Map<String, String> attributes = new HashMap<>();

      attributes.put(TIMESTAMP_KEY, String.valueOf(eventTime.getMillis()));

      System.out.println(String.format(
        "Publishing %s with attributes " + attributes + " event time: %s | processing time: %s with delay %d",
        jsonObject, eventTime, processingTime, delay));

      PubsubUtil.publish(
        PubsubUtil.PublishInput.builder()
          .objectToBePublished(jsonObject)
          .projectTopicName(TOPIC_NAME)
          .attributes(attributes)
          .build()
      );
    }
  }

  /**
   * This example best to be run at near the 5 minutes window.
   * e.g.
   * 10.00..10.05 pick 10.00
   * 12.15..12.20 pick 12.15
   * 21.00..21.05 pick 21.00
   * <p>
   * Assuming now is : 10:00:00
   * And the window is 5 one minutes with allowed lateness 5 minutes after.
   * <p>
   * Key (freeway) | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 10:06:00  <- late
   * 5             | 20                 | 10:04:10   | 10:05:15  <- late
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
   * Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 300                | 6                 | true    | true   | ON_TIME
   * <p>
   * ================================================================================================
   * WITH ALLOWED LATENESS (5 Minutes Lateness, Trigger Forever after end of window, Discarding Pane)
   * ================================================================================================
   * Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 300                | 6                 | true    | false  | ON_TIME
   * 5             | 20                 | 1                 | false   | false  | LATE
   * 5             | 20                 | 1                 | false   | false  | LATE
   * 5             | 30                 | 1                 | false   | false  | LATE
   * 5             | 60                 | 1                 | false   | false  | LATE
   * =================================================================================================
   * SPECULATIVE (5 Minutes Lateness, Trigger forever after first element in pane every 1 minute)
   * =================================================================================================
   * Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 50                 | 1                 | true    | false  | EARLY
   * 5             | 140                | 2                 | false   | false  | EARLY
   * 5             | 180                | 1                 | false   | false  | EARLY
   * 5             | 240                | 1                 | false   | false  | EARLY
   * 5             | 300                | 1                 | false   | false  | EARLY
   * 5             | 320                | 1                 | false   | false  | LATE
   * 5             | 340                | 1                 | false   | false  | LATE
   * 5             | 370                | 1                 | false   | false  | LATE
   * 5             | 430                | 1                 | false   | false  | LATE
   * =================================================================================================
   * SEQUENTIAL (5 Minutes Lateness, Trigger forever after first element in pane every 1 minute,
   * for late data firing every 2 minutes)
   * =================================================================================================
   * Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
   * 5             | 50                 | 1                 | true    | false  | EARLY
   * 5             | 140                | 2                 | false   | false  | EARLY
   * 5             | 180                | 1                 | false   | false  | EARLY
   * 5             | 240                | 1                 | false   | false  | EARLY
   * 5             | 300                | 1                 | false   | false  | ON_TIME
   * 5             | 370                | 3                 | false   | false  | LATE
   * 5             | 430                | 1                 | false   | false  | LATE
   * <p>
   * Note : Please run this example using google dataflow because the SDK have some issue that the watermark didn't
   * advancing for pubsub on direct runner
   */

  private static final DateTime now = DateTime.now();
  private static final DateTime roundFloor = now.minuteOfDay().roundFloorCopy();
  private static final Instant previousMinute = roundFloor.toInstant();

  private static final List<OutputMessage> outputMessages =
    Arrays.asList(
      new OutputMessage("5", 50,
        previousMinute.plus(3 * SECONDS),
        previousMinute.plus(47 * SECONDS)),
      new OutputMessage("5", 30,
        previousMinute.plus(MINUTES),
        previousMinute.plus(MINUTES + 3 * SECONDS)),
      new OutputMessage("5", 30,
        previousMinute.plus(2 * MINUTES),
        previousMinute.plus(6 * MINUTES)),
      new OutputMessage("5", 20,
        previousMinute.plus(4 * MINUTES + 10 * SECONDS),
        previousMinute.plus(5 * MINUTES + 15 * SECONDS)),
      new OutputMessage("5", 60,
        previousMinute.plus(3 * MINUTES + 59 * SECONDS),
        previousMinute.plus(4 * MINUTES + 5 * SECONDS)),
      //================================================5 DATA=========================================================
      new OutputMessage("5", 20,
        previousMinute.plus(3 * MINUTES + SECONDS),
        previousMinute.plus(5 * MINUTES + 30 * SECONDS)),
      new OutputMessage("5", 60,
        previousMinute.plus(MINUTES),
        previousMinute.plus(MINUTES + 15 * SECONDS)),
      new OutputMessage("5", 40,
        previousMinute.plus(2 * MINUTES + 40 * SECONDS),
        previousMinute.plus(2 * MINUTES + 43 * SECONDS)),
      new OutputMessage("5", 60,
        previousMinute.plus(3 * MINUTES + 20 * SECONDS),
        previousMinute.plus(7 * MINUTES + 25 * SECONDS)),
      new OutputMessage("5", 80,
        previousMinute.plus(2 * MINUTES),
        previousMinute.plus(3 * MINUTES)),
      //================================================10 DATA========================================================
      new OutputMessage("5", 100,
        previousMinute.plus(MINUTES),
        previousMinute.plus(11 * MINUTES))
    );

  public static void main(String[] args) {
    outputMessages.forEach(Thread::start);

    PubsubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).withValidation().as(PubsubToBigQuery.Options.class);

    options.setRunner(RUNNER);
    options.setJobName(SERIES);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<PubsubMessage> messages = pipeline.apply(
      "ReadPubSubSubscription",
      PubsubIO.readMessages()
        .withTimestampAttribute(TIMESTAMP_KEY)
        .fromSubscription(SUBSCRIPTION_NAME.toString()));

    PCollectionList<TableRow> resultList = messages.apply(ParDo.of(new EmitAndAddTimestamp()))
      .apply(new TriggerCollection.CalculateTotalFlow(
        WINDOW_DURATION,
        ALLOWED_LATENESS,
        TRIGGER_EVERY,
        TRIGGER_EVERY_AFTER_LATENESS
      ));

    for (int i = 0; i < resultList.size(); i++) {
      TableReference tableRef =
        TriggerCollection.getTableReference(
          ExampleUtils.PROJECT_ID,
          DATASET,
          "TriggerExampleSeriesOne_" + DebugTriggerExample.triggerTypes.get(i));

      System.out.println("Setting up table " + tableRef.toString());
      resultList.get(i).apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(TriggerCollection.getSchema())
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    }

    pipeline.run();
  }

  public static class EmitAndAddTimestamp extends DoFn<PubsubMessage, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      String messageJson = new String(message.getPayload(), StandardCharsets.UTF_8);

      System.out.println("Receiving message: " + messageJson + " event time " + context.timestamp().toString()
        + " | at " + Instant.now());

      Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
      InputMessage inputMessage = gson.fromJson(messageJson, InputMessage.class);

      KV<String, Integer> kv = KV.of(inputMessage.key, inputMessage.value);
      context.output(kv);
    }
  }
}
