package beam.example.state;

import beam.example.basic.PubsubToBigQuery;
import beam.example.state.constant.MessageAttribute;
import beam.example.state.entity.SuspiciousTransactionInformation;
import beam.example.state.entity.Transaction;
import beam.example.state.entity.TransactionPublisher;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static beam.example.time.TimeMultiplier.MINUTES;
import static beam.example.time.TimeMultiplier.SECONDS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public class StateExampleSeriesOne {
  private static final int WINDOW_DURATION = 1;
  private static final Class RUNNER = DirectRunner.class;
  private static final String SERIES = "state-example-series-one";

  private static final ProjectTopicName TOPIC_NAME_SERIES_ONE =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME_SERIES_ONE =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

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
   * t1            | 1             | 500000             | m1              | 10:00:01   | 10:00:01
   * t2            | 1             | 700000             | m1              | 10:00:03   | 10:00:03
   * t3            | 1             | 300000             | m2              | 10:00:05   | 10:00:05
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

  static final Instant nextWindow = TriggerCollection.getNearestWindowOf(WINDOW_DURATION);

  static final List<Transaction> userTransactions = Arrays.asList(
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

  /**
   * Add more noise, you can try to remove this and view the difference between them, the watermark never advance so
   * the default and allowedLateness could get all of the data. And the other method (speculative and sequential)
   * will showing data as EARLIER.
   */

  public static void main(String[] args) {
    userTransactions.forEach(transaction -> new TransactionPublisher(transaction).start(TOPIC_NAME_SERIES_ONE));

    System.out.println("The example will start ingesting the data at " + nextWindow.toString());

    PubsubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).withValidation().as(PubsubToBigQuery.Options.class);

    options.setRunner(RUNNER);
    options.setJobName(SERIES + "-" + Instant.now().getMillis());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<Transaction> transactionPCollection = pipeline.apply(
      "ReadPubSubSubscription",
      PubsubIO.readMessages()
        .withTimestampAttribute(MessageAttribute.TIMESTAMP_KEY)
        .fromSubscription(SUBSCRIPTION_NAME_SERIES_ONE.toString()))
      .apply(ParDo.of(new EncodeToTransaction()));

    transactionPCollection
      .apply(
        "MapTransactionAsKv",
        MapElements.into(
          TypeDescriptors.kvs(
            TypeDescriptors.strings(), TypeDescriptor.of(Transaction.class)))
          .via((Transaction transaction) -> KV.of("", transaction)))
      .apply(ParDo.of(new DetectTransactionAmount(0L, 0L)));

    pipeline.run();
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
