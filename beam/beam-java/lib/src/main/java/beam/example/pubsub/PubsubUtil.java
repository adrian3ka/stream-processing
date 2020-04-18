package beam.example.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.Map;

public class PubsubUtil {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Builder
  public static class PublishInput {
    @NonNull
    public final ProjectTopicName projectTopicName;

    @NonNull
    public final JsonObject objectToBePublished;

    @NonNull
    public final Map<String, String> attributes;
  }

  @SneakyThrows
  public static void publish(PublishInput input) {
    Publisher publisher = Publisher.newBuilder(input.projectTopicName).build();

    ByteString byteString;
    byteString = ByteString.copyFromUtf8(input.objectToBePublished.toString());

    publisher.publish(PubsubMessage
      .newBuilder()
      .setData(byteString)
      .putAllAttributes(input.attributes)
      .build());

    publisher.shutdown();
  }
}
