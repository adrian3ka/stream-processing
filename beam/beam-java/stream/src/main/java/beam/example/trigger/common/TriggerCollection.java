package beam.example.trigger.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.text.SimpleDateFormat;
import java.util.*;

public class TriggerCollection {
  // Constants used in triggers.
  // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
  // ONE_MINUTE is used only with processing time before the end of the window
  public static final Duration ONE_MINUTE = Duration.standardMinutes(1);
  // FIVE_MINUTES is used only with processing time after the end of the window
  public static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
  public static final Duration ONE_DAY = Duration.standardDays(1);

  /**
   * This transform demonstrates using triggers to control when data is produced for each window
   * Consider an example to understand the results generated by each type of trigger. The example
   * uses "freeway" as the key. Event time is the timestamp associated with the data element and
   * processing time is the time when the data element gets processed in the pipeline. For freeway
   * 5, suppose there are 10 elements in the [10:00:00, 10:30:00) window.
   * Key (freeway) | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 11:07:00
   * 5             | 20                 | 10:04:10   | 10:05:15
   * 5             | 60                 | 10:05:00   | 11:03:00
   * 5             | 20                 | 10:05:01   | 11.07:30
   * 5             | 60                 | 10:15:00   | 10:27:15
   * 5             | 40                 | 10:26:40   | 10:26:43
   * 5             | 60                 | 10:27:20   | 10:27:25
   * 5             | 60                 | 10:29:00   | 11:11:00
   *
   * <p>Beam tracks a watermark which records up to what point in event time the data is complete.
   * For the purposes of the example, we'll assume the watermark is approximately 15m behind the
   * current processing time. In practice, the actual value would vary over time based on the
   * systems knowledge of the current delay and contents of the backlog (data that has not yet been
   * processed).
   *
   * <p>If the watermark is 15m behind, then the window [10:00:00, 10:30:00) (in event time) would
   * close at 10:44:59, when the watermark passes 10:30:00.
   */
  public static class CalculateTotalFlow extends PTransform<PCollection<KV<String, Integer>>, PCollectionList<TableRow>> {
    private int windowDuration;
    private Duration allowedLateness;
    private Duration triggerEvery;
    private Duration triggerEveryAfterLateness;

    public CalculateTotalFlow(
      int windowDuration,
      Duration allowedLateness,
      Duration triggerEvery,
      Duration triggerEveryAfterLateness
    ) {
      this.windowDuration = windowDuration;
      this.allowedLateness = allowedLateness;
      this.triggerEvery = triggerEvery;
      this.triggerEveryAfterLateness = triggerEveryAfterLateness;
    }

    @Override
    public PCollectionList<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      System.out.println("Window duration: " + windowDuration);
      System.out.println("Allowed lateness: " + allowedLateness);
      System.out.println("Trigger every: " + triggerEvery);
      System.out.println("Trigger every after lateness: " + triggerEveryAfterLateness);
      // Concept #1: The default triggering behavior
      // By default Beam uses a trigger which fires when the watermark has passed the end of the
      // window. This would be written {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.

      // The system also defaults to dropping late data -- data which arrives after the watermark
      // has passed the event timestamp of the arriving element. This means that the default trigger
      // will only fire once.

      // Each pane produced by the default trigger with no allowed lateness will be the first and
      // last pane in the window, and will be ON_TIME.

      // The results for the example above with the default trigger and zero allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | true   | ON_TIME

      // At 11:03:00 (processing time) the system watermark may have advanced to 10:54:00. As a
      // result, when the data record with event time 10:05:00 arrives at 11:03:00, it is considered
      // late, and dropped.

      PCollection<TableRow> defaultTriggerResults = flowInfo
        .apply("Default", Window
          // The default window duration values work well if you're running the default
          // input
          // file. You may want to adjust the window duration otherwise.
          .<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(windowDuration)))
          // The default trigger first emits output when the system's watermark passes
          // the end
          // of the window.
          .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
          // Late data is dropped
          .withAllowedLateness(Duration.ZERO)
          // Discard elements after emitting each pane.
          // With no allowed lateness and the specified trigger there will only be a
          // single
          // pane, so this doesn't have a noticeable effect. See concept 2 for more
          // details.
          .discardingFiredPanes())
        .apply(new TotalFlow("default"));

      // Concept #2: Late data with the default trigger
      // This uses the same trigger as concept #1, but allows data that is up to ONE_DAY late. This
      // leads to each window staying open for ONE_DAY after the watermark has passed the end of the
      // window. Any late data will result in an additional pane being fired for that same window.

      // The first pane produced will be ON_TIME and the remaining panes will be LATE.
      // To definitely get the last pane when the window closes, use
      // .withAllowedLateness(ONE_DAY, ClosingBehavior.FIRE_ALWAYS).

      // The results for the example above with the default trigger and ONE_DAY allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | false  | ON_TIME
      // 5             | 60                 | 1                 | false   | false  | LATE
      // 5             | 30                 | 1                 | false   | false  | LATE
      // 5             | 20                 | 1                 | false   | false  | LATE
      // 5             | 60                 | 1                 | false   | false  | LATE
      PCollection<TableRow> withAllowedLatenessResults = flowInfo
        .apply(
          "WithLateData",
          Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(this.windowDuration)))
            // Late data is emitted as it arrives
            .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
            // Once the output is produced, the pane is dropped and we start preparing the
            // next
            // pane for the window
            .discardingFiredPanes()
            // Late data is handled up allowedLateness parameter
            .withAllowedLateness(this.allowedLateness, Window.ClosingBehavior.FIRE_ALWAYS))
        .apply(new TotalFlow("withAllowedLateness"));

      // Concept #3: How to get speculative estimates
      // We can specify a trigger that fires independent of the watermark, for instance after
      // ONE_MINUTE of processing time. This allows us to produce speculative estimates before
      // all the data is available. Since we don't have any triggers that depend on the watermark
      // we don't get an ON_TIME firing. Instead, all panes are either EARLY or LATE.

      // We also use accumulatingFiredPanes to build up the results across each pane firing.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // 5             | 320                | 7                 | false   | false  | LATE
      // 5             | 370                | 9                 | false   | false  | LATE
      // 5             | 430                | 10                | false   | false  | LATE
      PCollection<TableRow> speculativeResults = flowInfo
        .apply(
          "Speculative",
          Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(this.windowDuration)))
            // Trigger fires every @triggerEvery minute.
            .triggering(
              Repeatedly.forever(
                AfterProcessingTime.pastFirstElementInPane()
                  // Speculative firing trigger every @triggerEvery parameters
                  .plusDelayOf(this.triggerEvery)))
            // After emitting each pane, it will continue accumulating the elements so
            // that each
            // approximation includes all of the previous data in addition to the newly
            // arrived
            // data.
            .accumulatingFiredPanes()
            .withAllowedLateness(allowedLateness))
        .apply(new TotalFlow("speculative"));

      // Concept #4: Combining late data and speculative estimates
      // We can put the previous concepts together to get EARLY estimates, an ON_TIME result,
      // and LATE updates based on late data.

      // Each time a triggering condition is satisfied it advances to the next trigger.
      // If there are new elements this trigger emits a window under following condition:
      // > Early approximations every minute till the end of the window.
      // > An on-time firing when the watermark has passed the end of the window
      // > Every five minutes of late data.

      // Every pane produced will either be EARLY, ON_TIME or LATE.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // [First pane fired after the end of the window]
      // 5             | 320                | 7                 | false   | false  | ON_TIME
      // 5             | 430                | 10                | false   | false  | LATE

      // For more possibilities of how to build advanced triggers, see {@link Trigger}.
      PCollection<TableRow> sequentialResults = flowInfo
        .apply(
          "Sequential",
          Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(windowDuration)))
            .triggering(
              AfterEach.inOrder(
                Repeatedly.forever(
                  AfterProcessingTime.pastFirstElementInPane()
                    // Speculative every @triggerEvery
                    .plusDelayOf(this.triggerEvery))
                  .orFinally(AfterWatermark.pastEndOfWindow()),
                Repeatedly.forever(
                  AfterProcessingTime.pastFirstElementInPane()
                    // Late data every @triggerEveryAfterLateness
                    .plusDelayOf(this.triggerEveryAfterLateness))))
            .accumulatingFiredPanes()
            // For up to @allowedLateness
            .withAllowedLateness(this.allowedLateness))
        .apply(new TotalFlow("sequential"));

      // Adds the results generated by each trigger type to a PCollectionList.
      return PCollectionList.of(defaultTriggerResults)
        .and(withAllowedLatenessResults)
        .and(speculativeResults)
        .and(sequentialResults);
    }
  }

  /**
   * Calculate total flow and number of records for each freeway and format the results to TableRow
   * objects, to save to BigQuery.
   */
  public static class TotalFlow
    extends PTransform<PCollection<KV<String, Integer>>, PCollection<TableRow>> {
    public final String triggerType;

    public TotalFlow(String triggerType) {
      this.triggerType = triggerType;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      PCollection<KV<String, Iterable<Integer>>> flowPerFreeway =
        flowInfo.apply(GroupByKey.create());

      PCollection<KV<String, String>> results = flowPerFreeway.apply(
        ParDo.of(
          new DoFn<KV<String, Iterable<Integer>>, KV<String, String>>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
              // Just for differencing in print
              String id = "[" + UUID.randomUUID().toString() + "]";
              System.out.println(id + "Total flow for trigger type: " + triggerType);

              Iterable<Integer> flows = c.element().getValue();
              Integer sum = 0;
              Long numberOfRecords = 0L;
              for (Integer value : flows) {
                sum += value;
                numberOfRecords++;

                System.out.println(id + "Processing " + value + " become: " + sum + "," + numberOfRecords
                  + " | event time: " + c.timestamp()
                  + " | processing time: " + Instant.now().toString()
                  + " | timing: " + c.pane().getTiming()
                  + " | index:" + c.pane().getIndex()
                  + " | speculativeIndex: " + c.pane().getNonSpeculativeIndex());
              }
              c.output(KV.of(c.element().getKey(), sum + "," + numberOfRecords));
            }
          }));
      PCollection<TableRow> output = results.apply(ParDo.of(new FormatTotalFlow(triggerType)));
      return output;
    }
  }

  /**
   * Format the results of the Total flow calculation to a TableRow, to save to BigQuery. Adds the
   * triggerType, pane information, processing time and the window timestamp.
   */
  public static class FormatTotalFlow extends DoFn<KV<String, String>, TableRow> {
    private String triggerType;

    public FormatTotalFlow(String triggerType) {
      this.triggerType = triggerType;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      String[] values = c.element().getValue().split(",", -1);
      TableRow row =
        new TableRow()
          .set("trigger_type", triggerType)
          .set("freeway", c.element().getKey())
          .set("total_flow", Integer.parseInt(values[0]))
          .set("number_of_records", Long.parseLong(values[1]))
          .set("index", c.pane().getIndex())
          .set("window", window.toString())
          .set("isFirst", c.pane().isFirst())
          .set("isLast", c.pane().isLast())
          .set("timing", c.pane().getTiming().toString())
          .set("event_time", c.timestamp().toString())
          .set("processing_time", Instant.now().toString());

      System.out.println("-> FormatTotalFlow, with table row " + row.toString());
      c.output(row);
    }
  }

  /**
   * Sets the table reference.
   */
  public static TableReference getTableReference(String project, String dataset, String table) {
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(project);
    tableRef.setDatasetId(dataset);
    tableRef.setTableId(table);
    return tableRef;
  }

  /**
   * Defines the BigQuery schema used for the output.
   */
  public static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("trigger_type").setType("STRING"));
    fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("number_of_records").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("index").setType("INTEGER")); // firing index for the current window pane
    fields.add(new TableFieldSchema().setName("window").setType("STRING"));
    fields.add(new TableFieldSchema().setName("isFirst").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("isLast").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
    fields.add(new TableFieldSchema().setName("event_time").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
    return new TableSchema().setFields(fields);
  }

  public static Instant getNearestWindowOf(int windowMinute) {
    Calendar c = Calendar.getInstance();
    c.setTime(Date.from(java.time.Instant.now()));

    int minute = c.get(Calendar.MINUTE);
    int modMinute = minute % windowMinute;

    int minuteToAdd = windowMinute - modMinute;
    c.add(Calendar.MINUTE, minuteToAdd);

    Instant nextWindow = Instant.ofEpochMilli(c.getTime().toInstant().toEpochMilli());

    DateTime nearestDateTime = DateTime.parse(nextWindow.toString());
    DateTime roundFloor = nearestDateTime.minuteOfDay().roundFloorCopy();

    System.out.println(String.format("Next window minutes: %s", roundFloor));

    return roundFloor.toInstant();
  }
}
