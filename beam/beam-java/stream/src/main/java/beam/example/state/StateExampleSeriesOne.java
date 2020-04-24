package beam.example.state;

import beam.example.basic.PubsubToBigQuery;
import beam.example.state.constant.MessageAttribute;
import beam.example.state.entity.SuspiciousTransactionInformation;
import beam.example.state.entity.Transaction;
import beam.example.state.entity.TransactionPublisher;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static beam.example.time.TimeMultiplier.MINUTES;
import static beam.example.time.TimeMultiplier.SECONDS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public class StateExampleSeriesOne {
  private static final int WINDOW_DURATION = 1;
  private static final Class RUNNER = DirectRunner.class;
  private static final String SERIES = "state-example-series-one";

  private static final String DATASET = "cookbook";

  private static final String TRANSACTION_ACCUMULATION_KEY = "transaction-accumulation-key";

  private static final ProjectTopicName TOPIC_NAME_SERIES_ONE =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME_SERIES_ONE =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

  private static final Duration gapDuration = Duration.standardSeconds(30);
  private static final int transactionCountMarkAsSuspicious = 3;

  /**
   * We are trying to create anomaly detection
   * Assuming now is : 10:00:00
   * <p>
   * We mark the user have a burst transaction within 30 seconds gap, if there is more than
   * 3 transaction in a row or the total amount is increase significantly based on his / her average
   * we mark it as suspicious.
   * <p>
   * We mark to WATCH the user if their transaction is 2.5 times greater than global transaction
   * <p>
   * transactionId | userId        | total              | merchantId      | event time | processing time
   * t1            | 1             | 500000             | m1              | 10:00:01   | 10:00:01  <- suspicious transaction, burst transaction
   * t2            | 1             | 700000             | m1              | 10:00:03   | 10:00:03  <- suspicious transaction, burst transaction
   * t3            | 1             | 300000             | m2              | 10:00:05   | 10:00:05  <- suspicious transaction, burst transaction
   * t4            | 1             | 200000             | m2              | 10:00:10   | 10:00:10  <- suspicious transaction, burst transaction
   * t5            | 1             | 600000             | m3              | 10:00:15   | 10:00:15  <- suspicious transaction, burst transaction
   * <p>
   * t6            | 2             | 250000             | m2              | 10:00:30   | 10.01:35
   * t7            | 2             | 400000             | m2              | 10:01:30   | 10:01:35
   * t8            | 2             | 500000             | m3              | 10:02:30   | 10:02:43
   * t9            | 2             | 400000             | m3              | 10:03:30   | 10:03:30
   * t10           | 2             | 2600000            | m3              | 10:04:00   | 10:04:00  <- suspicious transaction, huge amount
   * <p>
   */

  private static final Instant nextWindow = TriggerCollection.getNearestWindowOf(WINDOW_DURATION);

  private static final List<Transaction> userTransactions = Arrays.asList(
    Transaction.builder().transactionId("t1").userId("1").total(500000L)
      .merchantId("m1")
      .eventTime(nextWindow.plus(SECONDS))
      .processingTime(nextWindow.plus(SECONDS)).build(),
    Transaction.builder().transactionId("t2").userId("1").total(700000L)
      .merchantId("m1")
      .eventTime(nextWindow.plus(3 * SECONDS))
      .processingTime(nextWindow.plus(3 * SECONDS)).build(),
    Transaction.builder().transactionId("t3").userId("1").total(300000L)
      .merchantId("m2")
      .eventTime(nextWindow.plus(5 * SECONDS))
      .processingTime(nextWindow.plus(5 * SECONDS)).build(),
    Transaction.builder().transactionId("t4").userId("1").total(200000L)
      .merchantId("m2")
      .eventTime(nextWindow.plus(10 * SECONDS))
      .processingTime(nextWindow.plus(10 * SECONDS)).build(),
    Transaction.builder().transactionId("t5").userId("1").total(600000L)
      .merchantId("m3")
      .eventTime(nextWindow.plus(15 * SECONDS))
      .processingTime(nextWindow.plus(15 * SECONDS)).build(),
    //==================================5 DATA======================================
    Transaction.builder().transactionId("t6").userId("2").total(250000L)
      .merchantId("m2")
      .eventTime(nextWindow.plus(30 * SECONDS))
      .processingTime(nextWindow.plus(MINUTES + 35 * SECONDS)).build(),
    Transaction.builder().transactionId("t7").userId("2").total(400000L)
      .merchantId("m2")
      .eventTime(nextWindow.plus(MINUTES + 30 * SECONDS))
      .processingTime(nextWindow.plus(MINUTES + 35 * SECONDS)).build(),
    Transaction.builder().transactionId("t8").userId("2").total(500000L)
      .merchantId("m3")
      .eventTime(nextWindow.plus(2 * MINUTES + 30 * SECONDS))
      .processingTime(nextWindow.plus(2 * MINUTES + 43 * SECONDS)).build(),
    Transaction.builder().transactionId("t9").userId("2").total(400000L)
      .merchantId("m3")
      .eventTime(nextWindow.plus(3 * MINUTES + 30 * SECONDS))
      .processingTime(nextWindow.plus(3 * MINUTES + 30 * SECONDS)).build(),
    Transaction.builder().transactionId("t10").userId("2").total(2600000L)
      .merchantId("m3")
      .eventTime(nextWindow.plus(4 * MINUTES))
      .processingTime(nextWindow.plus(4 * MINUTES)).build()
    //==================================10 DATA======================================
  );

  public static void main(String[] args) {
    TableReference tableRef =
      TriggerCollection.getTableReference(
        ExampleUtils.PROJECT_ID,
        DATASET,
        "StateExampleSeriesOne"
      );

    userTransactions.forEach(transaction -> new TransactionPublisher(transaction).start(TOPIC_NAME_SERIES_ONE));

    System.out.println("The example will start ingesting the data at " + nextWindow.toString());

    PubsubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).as(PubsubToBigQuery.Options.class);

    options.setRunner(RUNNER);
    options.setJobName(SERIES + "-" + Instant.now().getMillis());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<Transaction> transactionPCollection = pipeline.apply(
      "ReadPubSubSubscription",
      PubsubIO.readMessages()
        .withTimestampAttribute(MessageAttribute.TIMESTAMP_KEY)
        .fromSubscription(SUBSCRIPTION_NAME_SERIES_ONE.toString()))
      .apply(ParDo.of(new EncodeToTransaction()));

    // All transaction will be mapped into one key so all of the transaction will be accumulated on the @State
    transactionPCollection
      .apply(
        "MapTransactionAsStaticKv",
        MapElements.into(
          TypeDescriptors.kvs(
            TypeDescriptors.strings(), TypeDescriptor.of(Transaction.class)))
          .via((Transaction transaction) -> KV.of(TRANSACTION_ACCUMULATION_KEY, transaction)))
      .apply(ParDo.of(new DetectTransactionAmount(0L, 0L)))
      .apply("ConvertSuspiciousTransactionInformationToRowForHugeAmount",
        ParDo.of(new ConvertSuspiciousTransactionInformationToRow()))
      .apply(
        BigQueryIO.writeTableRows().to(tableRef).withSchema(SuspiciousTransactionInformation.getBigQuerySchema())
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      );

    transactionPCollection
      .apply(
        "MapTransactionByUserId",
        MapElements.into(
          TypeDescriptors.kvs(
            TypeDescriptors.strings(), TypeDescriptor.of(Transaction.class)))
          .via((Transaction transaction) -> KV.of(transaction.userId, transaction)))
      .apply(new DetectBurstTransaction(gapDuration, transactionCountMarkAsSuspicious))
      .apply("ConvertSuspiciousTransactionInformationToRowForBurstTransaction",
        ParDo.of(new ConvertSuspiciousTransactionInformationToRow()))
      .apply(
        BigQueryIO.writeTableRows().to(tableRef).withSchema(SuspiciousTransactionInformation.getBigQuerySchema())
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      );

    System.out.println("Prepare for running pipeline.....");

    pipeline.run();
  }

  public static class ConvertSuspiciousTransactionInformationToRow extends DoFn<SuspiciousTransactionInformation, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      SuspiciousTransactionInformation suspiciousTransactionInformation = context.element();

      context.output(suspiciousTransactionInformation.getTableRow(window.toString()));
    }
  }


  // Maybe you can use state map instead of window wih minimal elements count 3. But please be wise
  // the state is saved on memory and it very risky to have memory leak issue if you mapped it by user id
  // because we never know the boundary how would be very large our user is.
  // The state will be never taken out if it unused for amount of time, but if we use triggering and window
  // it will garbage collected. So it very possibly of memory leak.
  public static class DetectBurstTransaction extends PTransform<PCollection<KV<String, Transaction>>, PCollection<SuspiciousTransactionInformation>> {
    private Duration gapDuration;
    private int suspiciousTransactionCount;

    DetectBurstTransaction(
      Duration gapDuration,
      int suspiciousTransactionCount
    ) {
      this.gapDuration = gapDuration;
      this.suspiciousTransactionCount = suspiciousTransactionCount;
    }

    private class ConvertToSuspiciousTransactionInformation extends PTransform<PCollection<KV<String, Transaction>>, PCollection<SuspiciousTransactionInformation>> {

      @Override
      public PCollection<SuspiciousTransactionInformation> expand(PCollection<KV<String, Transaction>> transactionMappedByUserId) {
        PCollection<KV<String, Iterable<Transaction>>> flowPerKey =
          transactionMappedByUserId.apply(GroupByKey.create());

        PCollection<SuspiciousTransactionInformation> results = flowPerKey.apply(
          ParDo.of(
            new DoFn<KV<String, Iterable<Transaction>>, SuspiciousTransactionInformation>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                // Just for differencing in print
                String id = "[" + UUID.randomUUID().toString() + "]";

                Iterable<Transaction> flows = context.element().getValue();
                int transactionCount = 0;

                for (Transaction transaction : flows) {
                  transactionCount++;
                  System.out.println(id + "Processing "
                    + " userId=" + context.element().getKey()
                    + " | event time: " + context.timestamp()
                    + " | processing time: " + Instant.now().toString()
                    + " | timing: " + context.pane().getTiming()
                    + " | index:" + context.pane().getIndex()
                    + " | transactionCount: " + transactionCount);

                  String reason = String.format("Transaction id %s for user: %s marked as suspicious" +
                      " because it have a burst transaction in session with gap duration %s",
                    transaction.transactionId,
                    transaction.userId,
                    gapDuration
                  );

                  System.out.println(reason);

                  context.output(SuspiciousTransactionInformation.builder()
                    .transactionId(transaction.transactionId)
                    .eventTime(context.timestamp())
                    .reason(reason).build());
                }
              }
            }));

        return results;
      }
    }

    @Override
    public PCollection<SuspiciousTransactionInformation> expand(PCollection<KV<String, Transaction>> transactionMappedByUserId) {
      System.out.println("Setting up with pipeline for DetectBurstTransaction...");
      System.out.println("-> Gap duration: " + gapDuration);
      System.out.println("-> Suspicious transaction count: " + suspiciousTransactionCount);

      return transactionMappedByUserId.apply("TransactionPerSession",
        Window.<KV<String, Transaction>>into(
          Sessions.withGapDuration(gapDuration))
          .triggering(
            AfterEach.inOrder(
              // The next transaction will assumed as suspicious
              AfterPane.elementCountAtLeast(suspiciousTransactionCount + 1),
              // All of the next transaction after that will be marked as suspicious
              Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
          )
          .withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes()
      ).apply(new ConvertToSuspiciousTransactionInformation());
    }
  }

  public static class DetectTransactionAmount extends DoFn<KV<String, Transaction>, SuspiciousTransactionInformation> {
    private final String GRAND_TOTAL = "grandTotal";
    private final String TRANSACTION_COUNT = "transactionCount";

    private final double MAXIMUM_MULTIPLIER = 2.5;

    @StateId(GRAND_TOTAL)
    private final StateSpec<ValueState<Long>> grandTotalAmount = StateSpecs.value(VarLongCoder.of());

    @StateId(TRANSACTION_COUNT)
    private final StateSpec<ValueState<Long>> transactionCount = StateSpecs.value(VarLongCoder.of());

    long initialGrandTotal;
    long initialTransactionCount;

    public DetectTransactionAmount() {
      new DetectTransactionAmount(0L, 0L);
    }

    public DetectTransactionAmount(
      Long initialGrandTotal,
      Long initialTransactionCount
    ) {
      this.initialGrandTotal = initialGrandTotal;
      this.initialTransactionCount = initialTransactionCount;
    }

    @ProcessElement
    public void processElement(
      ProcessContext context,
      @StateId(GRAND_TOTAL) ValueState<Long> grandTotal,
      @StateId(TRANSACTION_COUNT) ValueState<Long> transactionCount
    ) {
      Transaction transaction = context.element().getValue();

      // ValueState cells do not contain a default value. If the state is possibly not written, make
      // sure to check for null on read.
      grandTotal.write(firstNonNull(grandTotal.read(), initialGrandTotal) + transaction.total);

      transactionCount.write(firstNonNull(transactionCount.read(), initialGrandTotal) + 1);
      long averageCurrentTotalTransaction = grandTotal.read() / transactionCount.read();

      System.out.println("Transaction total " + transaction.total
        + " | average transaction " + averageCurrentTotalTransaction
        + " | transactionCount " + transactionCount.read());

      if (transaction.total > averageCurrentTotalTransaction * MAXIMUM_MULTIPLIER) {
        String reason = String.format("Transaction amount is %d, 2.5 times greater than current average %d",
          transaction.total,
          averageCurrentTotalTransaction
        );

        System.out.println(reason);

        // You can output to bigquery / pubsub or any other storage
        context.output(
          SuspiciousTransactionInformation.builder()
            .reason(reason)
            .transactionId(transaction.transactionId)
            .eventTime(context.timestamp())
            .build()
        );
      }
    }
  }

  public static class EncodeToTransaction extends DoFn<PubsubMessage, Transaction> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      String messageJson = new String(message.getPayload(), StandardCharsets.UTF_8);

      System.out.println("Receiving message: " + messageJson + " event time " + context.timestamp().toString()
        + " | at " + Instant.now());

      Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
      Transaction transaction = gson.fromJson(messageJson, Transaction.class);

      context.output(transaction);
    }
  }
}
