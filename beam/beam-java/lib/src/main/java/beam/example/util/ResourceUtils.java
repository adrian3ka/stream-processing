/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package beam.example.util;

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

/**
 * The {@link ResourceUtils} class provides helper methods for handling common resources.
 */
public class ResourceUtils {
  /**
   * The log to output status messages to.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  /**
   * The path within resources to the dead-letter BigQuery schema.
   */
  private static final String DEADLETTER_SCHEMA_FILE_PATH = "schema/streaming_source_deadletter_table_schema.json";
  private static final String PUBSUB_TO_BIGQUERY_SCHEMA_FILE_PATH =
    "schema/streaming_pubsub_to_bigquery_table_schema.json";

  /**
   * Retrieves the file contents of the dead-letter schema file within the project's resources into
   * a {@link String} object.
   *
   * @return The schema JSON string.
   */
  public static String getDeadletterTableSchemaJson() {
    String schemaJson = null;
    try {
      schemaJson =
        Resources.toString(
          Resources.getResource(DEADLETTER_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOG.error(
        "Unable to read {} file from the resources folder!", DEADLETTER_SCHEMA_FILE_PATH, e);
    }

    return schemaJson;
  }

  public static String getPubsubToBigQueryTableSchemaJson() {
    String schemaJson = null;
    try {
      schemaJson =
        Resources.toString(
          Resources.getResource(PUBSUB_TO_BIGQUERY_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOG.error(
        "Unable to read {} file from the resources folder!", DEADLETTER_SCHEMA_FILE_PATH, e);
    }

    return schemaJson;
  }

  public static String getData(String name) {
    URL resource = ResourceUtils.class.getResource("/data/" + name);
    try {
      return Paths.get(resource.toURI()).toFile().getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
