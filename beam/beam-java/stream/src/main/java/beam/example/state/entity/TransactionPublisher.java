package beam.example.state.entity;


import beam.example.pubsub.PubsubUtil;
import beam.example.state.constant.MessageAttribute;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.ProjectTopicName;
import lombok.SneakyThrows;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

public class TransactionPublisher extends Thread {
  final Transaction transaction;

  private ProjectTopicName projectTopicName;

  public TransactionPublisher(Transaction transaction) {
    this.transaction = transaction;
  }

  public void start(ProjectTopicName topicName) {
    projectTopicName = topicName;
    this.start();
  }

  @SneakyThrows
  public void run() {
    Instant eventTime = transaction.eventTime;
    Instant processingTime = transaction.processingTime;

    if (projectTopicName == null) {
      throw new RuntimeException("Please start using Project Topic Name");
    }

    long delay = transaction.processingTime.getMillis() - Instant.now().getMillis();

    Gson gson = new Gson();

    // JSON data structure
    JsonObject jsonObject = gson.toJsonTree(transaction).getAsJsonObject();

    Map<String, String> attributes = new HashMap<>();

    attributes.put(MessageAttribute.TIMESTAMP_KEY, String.valueOf(transaction.eventTime.getMillis()));

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