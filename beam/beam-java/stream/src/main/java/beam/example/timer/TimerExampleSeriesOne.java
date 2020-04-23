package beam.example.timer;

import beam.example.basic.PubsubToBigQuery;
import beam.example.state.constant.MessageAttribute;
import beam.example.timer.entity.TimerDelay;
import beam.example.timer.entity.TimerDelayPublisher;
import beam.example.trigger.common.ExampleUtils;
import beam.example.trigger.common.TriggerCollection;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static beam.example.time.TimeMultiplier.SECONDS;

public class TimerExampleSeriesOne {

  private static final int WINDOW_DURATION = 1;
  private static final Class RUNNER = DirectRunner.class;
  private static final String SERIES = "timer-example-series-one";

  private static final ProjectTopicName TOPIC_NAME_SERIES_ONE =
    ProjectTopicName.of(ExampleUtils.PROJECT_ID, SERIES);

  private static final ProjectSubscriptionName SUBSCRIPTION_NAME_SERIES_ONE =
    ProjectSubscriptionName.of(ExampleUtils.PROJECT_ID, SERIES + "-subscription");

  private static final Instant nextWindow = TriggerCollection.getNearestWindowOf(WINDOW_DURATION);

  private static final List<TimerDelay> userTransactions = Arrays.asList(
    TimerDelay.builder().delayInSeconds(10)
      .timerId("1")
      .eventTime(nextWindow.plus(10 * SECONDS))
      .processingTime(nextWindow.plus(10 * SECONDS))
      .build()
    //==================================10 DATA======================================
  );

  public static void main(String[] args) {
    userTransactions.forEach(transaction -> new TimerDelayPublisher(transaction).start(TOPIC_NAME_SERIES_ONE));

    System.out.println("The example will start ingesting the data at " + nextWindow.toString());

    PubsubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).withValidation().as(PubsubToBigQuery.Options.class);

    options.setRunner(RUNNER);
    options.setJobName(SERIES + "-" + Instant.now().getMillis());

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(
      "ReadPubSubSubscription",
      PubsubIO.readMessages()
        .withTimestampAttribute(MessageAttribute.TIMESTAMP_KEY)
        .fromSubscription(SUBSCRIPTION_NAME_SERIES_ONE.toString()))
      .apply(ParDo.of(new ConvertPubsubMessageToTimerDelay()))
      .apply(
        "MyTimerDelayByTimerId",
        MapElements.into(
          TypeDescriptors.kvs(
            TypeDescriptors.strings(), TypeDescriptor.of(TimerDelay.class)))
          .via((TimerDelay timerDelay) -> KV.of(timerDelay.timerId, timerDelay)))
      .apply(ParDo.of(new MyTimer()));

    System.out.println("Prepare for running pipeline.....");
    pipeline.run();
  }

  public static class ConvertPubsubMessageToTimerDelay extends DoFn<PubsubMessage, TimerDelay> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      String messageJson = new String(message.getPayload(), StandardCharsets.UTF_8);

      System.out.println("Receiving message: " + messageJson + " event time " + context.timestamp().toString()
        + " | at " + Instant.now());

      Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
      TimerDelay timerDelay = gson.fromJson(messageJson, TimerDelay.class);

      context.output(timerDelay);
    }
  }

  public static class MyTimer extends DoFn<KV<String, TimerDelay>, TimerDelay> {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(
      ProcessContext context,
      @TimerId("timer") Timer timer
    ) {
      TimerDelay timerDelay = context.element().getValue();

      timer.offset(timerDelay.getDelayDuration()).setRelative();

      context.output(context.element().getValue());
    }

    @OnTimer("timer")
    public void onTimer(
      @Element KV<String, TimerDelay> element,
      OutputReceiver<TimerDelay> output
    ) {
      System.out.println("Timer fired " + element.getValue().timerId);
    }
  }
}
