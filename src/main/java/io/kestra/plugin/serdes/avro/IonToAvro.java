package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.*;
import java.net.URI;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided file containing ion serialized data and convert it to avro."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a CSV file to the Avro format.",
            code = """     
id: divvy_tripdata
namespace: dev

variables:
  file_id: "{{ execution.startDate | dateAdd(-3, 'MONTHS') | date('yyyyMM') }}"

tasks:
  - id: get_zipfile
    type: io.kestra.plugin.core.http.Download
    uri: "https://divvy-tripdata.s3.amazonaws.com/{{ render(vars.file_id) }}-divvy-tripdata.zip"

  - id: unzip
    type: io.kestra.plugin.compress.ArchiveDecompress
    algorithm: ZIP
    from: "{{ outputs.get_zipfile.uri }}"

  - id: convert
    type: io.kestra.plugin.serdes.csv.CsvToIon
    from: "{{ outputs.unzip.files[render(vars.file_id) ~ '-divvy-tripdata.csv'] }}"

  - id: to_avro
    type: io.kestra.plugin.serdes.avro.IonToAvro
    from: "{{ outputs.convert.uri }}"
    datetimeFormat: "yyyy-MM-dd' 'HH:mm:ss"
    schema: |
      {
        "type": "record",
        "name": "Ride",
        "namespace": "com.example.bikeshare",
        "fields": [
          {"name": "ride_id", "type": "string"},
          {"name": "rideable_type", "type": "string"},
          {"name": "started_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
          {"name": "ended_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
          {"name": "start_station_name", "type": "string"},
          {"name": "start_station_id", "type": "string"},
          {"name": "end_station_name", "type": "string"},
          {"name": "end_station_id", "type": "string"},
          {"name": "start_lat", "type": "double"},
          {"name": "start_lng", "type": "double"},
          {
            "name": "end_lat",
            "type": ["null", "double"],
            "default": null
          },
          {
            "name": "end_lng",
            "type": ["null", "double"],
            "default": null
          },
          {"name": "member_casual", "type": "string"}
        ]
      }"""
        )
    },
    aliases = "io.kestra.plugin.serdes.avro.AvroWriter"
)
public class IonToAvro extends AbstractAvroConverter implements RunnableTask<IonToAvro.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.workingDir().createTempFile(".avro").toFile();
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro writer
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema, AvroConverter.genericData());

        // reader
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)));
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            DataFileWriter<GenericRecord> schemaDataFileWriter = dataFileWriter.create(schema, output)
        ) {
            Long lineCount = this.convert(inputStream, schema, dataFileWriter::append);

            // metrics & finalize
            runContext.metric(Counter.of("records", lineCount));

            schemaDataFileWriter.flush();
            dataFileWriter.flush();
            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }
}
