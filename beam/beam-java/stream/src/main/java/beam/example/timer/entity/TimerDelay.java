package beam.example.timer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Duration;
import org.joda.time.Instant;

@AllArgsConstructor
@Builder
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
public class TimerDelay {
  @NonNull
  public String timerId;

  @NonNull
  public int delayInSeconds;

  @NonNull
  public Instant eventTime;

  @NonNull
  public Instant processingTime;

  public Duration getDelayDuration () {
    return Duration.standardSeconds(delayInSeconds);
  }
}
