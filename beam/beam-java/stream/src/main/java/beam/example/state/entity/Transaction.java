package beam.example.state.entity;

import lombok.*;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

@AllArgsConstructor
@Builder
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
public class Transaction {
  @NonNull
  public String transactionId;

  @NonNull
  public String userId;

  @NonNull
  public Long total;

  @NonNull
  public String merchantId;

  @NonNull
  public Instant eventTime;

  @NonNull
  public Instant processingTime;
}
