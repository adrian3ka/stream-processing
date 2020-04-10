/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package beam.example.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An example that reads the public samples of weather data from BigQuery, counts the number of
 * tornadoes that occur in each month, and writes the results to BigQuery.
 *
 * <p>Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify the BigQuery table for the output with the form:
 *
 * <pre>{@code
 * --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 * <p>
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The BigQuery input table defaults to {@code clouddataflow-readonly:samples.weather_stations}
 * and can be overridden with {@code --input}.
 */
public class BigQueryTornadoes {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private static final String WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations";

  private static final String PROJECT_ID = "beam-tutorial-272917";

  // Please replace beam-tutorial-272917 with your own project name
  // also replace the cookbook with the data sets you already created
  // you can specify the last part with anything
  private static final String TORNADOES_OUTPUT = PROJECT_ID + ":cookbook.tornadoes";

  // Create your own storage first on google storage
  private static final String GCS_STAGING_LOCATION = "gs://beam-tutorial-272917/staging";
  private static final String GCS_TMP_LOCATION = "gs://beam-tutorial-272917/tmp";

  /**
   * Examines each row in the input table. If a tornado was recorded in that sample, the month in
   * which it occurred is output.
   */
  static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      if ((Boolean) row.get("tornado")) {
        c.output(Integer.parseInt((String) row.get("month")));
      }
    }
  }

  /**
   * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
   * representation of month and the number of tornadoes that occurred in each month.
   */
  static class FormatCountsFn extends DoFn<KV<Integer, Long>, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row =
        new TableRow()
          .set("month", c.element().getKey())
          .set("tornado_count", c.element().getValue() * 10);
      c.output(row);
    }
  }

  /**
   * Takes rows from a table and generates a table of counts.
   *
   * <p>The input schema is described by https://developers.google.com/bigquery/docs/dataset-gsod .
   * The output contains the total number of tornadoes found in each month in the following schema:
   *
   * <ul>
   *   <li>month: integer
   *   <li>tornado_count: integer
   * </ul>
   */
  static class CountTornadoes extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> rows) {

      // row... => month...
      PCollection<Integer> tornadoes = rows.apply(ParDo.of(new ExtractTornadoesFn()));

      // month... => <month,count>...
      PCollection<KV<Integer, Long>> tornadoCounts = tornadoes.apply(Count.perElement());

      // <month,count>... => row...
      PCollection<TableRow> results = tornadoCounts.apply(ParDo.of(new FormatCountsFn()));

      return results;
    }
  }

  /**
   * Options supported by {@link BigQueryTornadoes}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();

    void setInput(String value);

    @Description("Mode to use when reading from BigQuery")
    @Default.Enum("EXPORT")
    TypedRead.Method getReadMethod();

    void setReadMethod(TypedRead.Method value);

    @Description(
      "BigQuery table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Default.String(TORNADOES_OUTPUT)
    String getOutput();

    void setOutput(String value);
  }

  static void runBigQueryTornadoes(Options options) {
    Pipeline p = Pipeline.create(options);

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    PCollection<TableRow> rowsFromBigQuery;

    switch (options.getReadMethod()) {
      case DIRECT_READ:
        rowsFromBigQuery =
          p.apply(
            BigQueryIO.readTableRows()
              .from(options.getInput())
              .withMethod(Method.DIRECT_READ)
              .withSelectedFields(Lists.newArrayList("month", "tornado")));
        break;

      default:
        rowsFromBigQuery =
          p.apply(
            BigQueryIO.readTableRows()
              .from(options.getInput())
              .withMethod(options.getReadMethod()));
        break;
    }

    rowsFromBigQuery
      .apply(new CountTornadoes())
      .apply(
        BigQueryIO.writeTableRows()
          .to(options.getOutput())
          .withSchema(schema)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();
  }

  static String[] appendArgs(String[] args) {
    List<String> listArgs = new ArrayList<>(Arrays.asList(args));

    listArgs.add("--project=" + PROJECT_ID);

    String[] itemsArray = new String[listArgs.size()];
    itemsArray = listArgs.toArray(itemsArray);

    return itemsArray;
  }

  public static void main(String[] args) {
    // Dont forget to setup on IDE
    // GOOGLE_APPLICATION_CREDENTIALS=../../google-credentials/[certs-name].json

    Options options = PipelineOptionsFactory.fromArgs(
      appendArgs(args)
    ).withValidation().as(Options.class);
    options.setTempLocation(GCS_TMP_LOCATION);
    options.setRunner(DirectRunner.class);

    runBigQueryTornadoes(options);
  }
}
