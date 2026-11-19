package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SimpleDataflow {

  public interface SimpleDataflowOptions extends PipelineOptions {

    @Description("Pub/Sub subscription to read from")
    @Default.String(
        "projects/upbeat-math-480123-r1/subscriptions/sales-events-sub")
    String getSubscription();

    void setSubscription(String subscription);

    @Description("BigQuery destination in project:dataset.table format")
    @Default.String("upbeat-math-480123-r1:sales_dataset.streaming_sales")
    String getTable();

    void setTable(String table);

    @Description("GCS staging location")
    @Default.String("gs://dataflow_demo_bucket/temp")
    String getTempLocation();

    void setTempLocation(String tempLocation);
  }

  private static TableRow parseMessage(String message) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode json = objectMapper.readTree(message);

    return new TableRow()
        .set("product", json.path("product").asText())
        .set("sales", json.path("sales").asDouble());
  }

  public static void main(String[] args) {
    SimpleDataflowOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SimpleDataflowOptions.class);

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                List.of(
                    new TableFieldSchema().setName("product").setType("STRING"),
                    new TableFieldSchema().setName("sales").setType("FLOAT")));

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read from PubSub", PubsubIO.readStrings().fromSubscription(options.getSubscription()))
        .apply(
            "Parse JSON",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(
                    message -> {
                      try {
                        return parseMessage(message);
                      } catch (Exception exception) {
                        throw new RuntimeException("Unable to parse Pub/Sub message", exception);
                      }
                    }))
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getTable())
                .withSchema(tableSchema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

    pipeline.run();
  }
}
