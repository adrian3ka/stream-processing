package beam.example.basic;


import java.io.IOException;

import beam.example.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class GcsToPubsub {
  public static final String STREAMING_TOPIC = "projects/beam-tutorial-272917/topics/streaming";

  public interface GcsToPubsubOptions extends PipelineOptions {
    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("data/input1.txt")
    String getInputFile();

    void setInputFile(String value);
  }

  static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
      Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      // Split the line into words.
      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        System.out.println(word);
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  public static class PrintLine
    extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      return words;
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Preparing Options.......");
    GcsToPubsubOptions options = PipelineOptionsFactory.fromArgs(
      ExampleUtils.appendArgs(args)
    ).withValidation().as(GcsToPubsubOptions.class);

    Pipeline p = Pipeline.create(options);

    System.out.println("Preparing Publish.......");

    p.apply("Read GCS", TextIO.read().from(options.getInputFile()))
      .apply(new PrintLine())
      .apply("Sending Pub/sub", PubsubIO.writeStrings().to(STREAMING_TOPIC));

    PipelineResult result = p.run();
    try {
      result.waitUntilFinish();
    } catch (Exception exc) {
      result.cancel();
    }
  }
}
