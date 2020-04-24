package beam.example.state.entity;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Builder
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
@AllArgsConstructor
public class SuspiciousTransactionInformation implements Serializable {
  @NonNull
  public String transactionId;

  @NonNull
  public String reason;

  @NonNull
  public Instant eventTime;

  /**
   * Defines the BigQuery schema used for the output.
   */
  public static TableSchema getBigQuerySchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("transaction_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("reason").setType("STRING"));
    fields.add(new TableFieldSchema().setName("window").setType("STRING"));
    fields.add(new TableFieldSchema().setName("event_time").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
    return new TableSchema().setFields(fields);
  }

  public TableRow getTableRow(String window) {
    return new TableRow()
      .set("transaction_id", this.transactionId)
      .set("reason", this.reason)
      .set("window", window)
      .set("event_time", this.eventTime.toString())
      .set("created_at", Timestamp.now().toString());
  }
}
