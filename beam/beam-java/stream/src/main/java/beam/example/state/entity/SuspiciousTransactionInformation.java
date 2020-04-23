package beam.example.state.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@Builder
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
@AllArgsConstructor
public class SuspiciousTransactionInformation {
  @NonNull
  public String transactionId;

  @NonNull
  public String reason;
}
