package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.avro.infer.InferAvroSchema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import io.kestra.plugin.serdes.OnBadLines;
import org.apache.avro.SchemaParseException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.Set;

import java.io.*;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Convert an ION file into Avro."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a CSV file to the Avro format.",
            code = """
                id: divvy_tripdata
                namespace: company.team

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
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.avro.AvroWriter"
)
public class IonToAvro extends AbstractAvroConverter implements RunnableTask<IonToAvro.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @PluginProperty
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "How to handle bad records (e.g., null values in non-nullable fields or type mismatches).",
        description = "Can be one of: `FAIL`, `WARN` or `SKIP`."
    )
    private final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        OnBadLines rOnBadLinesValue = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".avro").toFile();

        // avro writer
        var schemaParser = new Schema.Parser();
        Schema schema = null;
        if (this.schema == null) {
            var inputStreamForInfer = new InputStreamReader(runContext.storage().getFile(rFrom));
            var schemaOutputStream = new ByteArrayOutputStream();
            new InferAvroSchema().inferAvroSchemaFromIon(inputStreamForInfer, schemaOutputStream);
            schema = schemaParser.parse(schemaOutputStream.toString());
        } else {
            try {
                schema = schemaParser.parse(runContext.render(this.schema));
            }
            catch (SchemaParseException e) {
                Set<ConstraintViolation<?>> violations = new HashSet<>(); // Simulate
                // Add violation for 'schema' property
                throw new ConstraintViolationException("Invalid Avro schema: " + e.getMessage(), violations);
            }
        }

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema, AvroConverter.genericData());

        try (
            Reader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(rFrom)), FileSerde.BUFFER_SIZE);
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            DataFileWriter<GenericRecord> schemaDataFileWriter = dataFileWriter.create(schema, output)
        ) {
            Long lineCount = this.convert(inputStream, schema, record -> {
                try {
                    dataFileWriter.append(record);
                } catch (Exception e) {
                    if (rOnBadLinesValue == OnBadLines.ERROR) {
                        throw e;
                    } else if (rOnBadLinesValue == OnBadLines.WARN) {
                        runContext.logger().warn("Bad record skipped (onBadLines=WARN): {}", e.getMessage());
                        return;
                    }
                    // OnBadLines.SKIP or others: silently skip
                }
            }, runContext);

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