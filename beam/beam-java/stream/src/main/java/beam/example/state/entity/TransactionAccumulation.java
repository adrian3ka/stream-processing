package beam.example.state.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@AllArgsConstructor
@Builder
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
public class TransactionAccumulation {
  @NonNull
  public Long grandTotal;

  @NonNull
  public Long transactionCount;
}
