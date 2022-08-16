package io.kestra.plugin.serdes.avro;

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
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided file containing ion serialized data and convert it to avro."
)
public class AvroWriter extends AbstractAvroConverter implements RunnableTask<AvroWriter.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.tempFile(".avro").toFile();
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro writer
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema, AvroConverter.genericData());

        // reader
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
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
            .uri(runContext.putTempFile(tempFile))
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
