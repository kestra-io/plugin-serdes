package io.kestra.plugin.serdes.parquet;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroDeserializer;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided parquet file and convert it to ion serialized data file."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a parquet file to the Amazon Ion format.",
            code = """
id: parquet_to_ion
namespace: company.team

tasks:
  - id: http_download
    type: io.kestra.plugin.core.http.Download
    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/parquet/products.parquet

  - id: to_ion
    type: io.kestra.plugin.serdes.parquet.ParquetToIon
    from: "{{ outputs.http_download.uri }}"
"""
        )
    },
    aliases = "io.kestra.plugin.serdes.parquet.ParquetReader"
)
public class ParquetToIon extends Task implements RunnableTask<ParquetToIon.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    private Property<String> from;

    static {
        ParquetTools.handleLogger();

        // We initialize snappy in a static initializer block, so it is done when the plugin is loaded by the plugin registry,
        // and not at when it is executed by the Worker to prevent issues with Java Security that prevent writing on /tmp.
        ParquetTools.initSnappy();
    }

    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // New ion file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // Parquet file
        File parquetFile = runContext.workingDir().createTempFile(".parquet").toFile();
        try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(parquetFile), FileSerde.BUFFER_SIZE)) {
            IOUtils.copyLarge(runContext.storage().getFile(from), outputStream);
        }
        Path parquetHadoopPath = new Path(parquetFile.getPath());
        HadoopInputFile parquetOutputFile = HadoopInputFile.fromPath(parquetHadoopPath, new Configuration());

        AvroParquetReader.Builder<GenericRecord> parquetReaderBuilder = AvroParquetReader.<GenericRecord>builder(parquetOutputFile)
            .disableCompatibility()
            .withDataModel(AvroConverter.genericData());

        try (
            org.apache.parquet.hadoop.ParquetReader<GenericRecord> parquetReader = parquetReaderBuilder.build();
            Writer output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {

            Flux<Map<String, Object>> flowable = Flux
                .create(this.nextRow(parquetReader), FluxSink.OverflowStrategy.BUFFER)
                .map(AvroDeserializer::recordDeserializer);

            Mono<Long> count = FileSerde.writeAll(output, flowable);
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private Consumer<FluxSink<GenericRecord>> nextRow(org.apache.parquet.hadoop.ParquetReader<GenericRecord> parquetReader) throws IOException {
        return throwConsumer(s -> {
            boolean next = true;
            while (next) {
                GenericRecord record = parquetReader.read();

                if (record == null) {
                    next = false;
                } else {
                    s.next(record);
                }
            }

            s.complete();
        });
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
