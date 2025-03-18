package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
    title = "Read a provided avro file and convert it to ion serialized data file."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an Avro file to the Amazon Ion format.",
            code = """
                id: avro_to_ion
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/avro/products.avro

                  - id: to_ion
                    type: io.kestra.plugin.serdes.avro.AvroToIon
                    from: "{{ outputs.http_download.uri }}"
                """
        )
    },
    aliases = "io.kestra.plugin.serdes.avro.AvroReader"
)
public class AvroToIon extends Task implements RunnableTask<AvroToIon.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // New ion file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (
            InputStream in = runContext.storage().getFile(from);
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(in, datumReader);

            Flux<Map<String, Object>> flowable = Flux
                .create(this.nextRow(dataFileStream), FluxSink.OverflowStrategy.BUFFER)
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

    private Consumer<FluxSink<GenericRecord>> nextRow(DataFileStream<GenericRecord> dataFileStream) throws IOException {
        return throwConsumer(s -> {
            GenericRecord record = null;
            while (dataFileStream.hasNext()) {
                record = dataFileStream.next(record);
                s.next(record);
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
