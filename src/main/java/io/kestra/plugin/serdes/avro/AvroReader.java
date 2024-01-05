package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.serializers.FileSerde;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import jakarta.validation.constraints.NotNull;
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
public class AvroReader extends Task implements RunnableTask<AvroReader.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // New ion file
        File tempFile = runContext.tempFile(".ion").toFile();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try(
            InputStream in = runContext.uriToInputStream(from);
            OutputStream output = new FileOutputStream(tempFile)
        ) {
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(in, datumReader);

            Flux<Map<String, Object>> flowable = Flux
                .create(this.nextRow(dataFileStream), FluxSink.OverflowStrategy.BUFFER)
                .map(AvroDeserializer::recordDeserializer)
                .doOnNext(throwConsumer(row -> FileSerde.write(output, row)));

            Mono<Long> count = flowable.count();
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    private Consumer<FluxSink<GenericRecord>> nextRow(DataFileStream<GenericRecord> dataFileStream) throws IOException {
        return throwConsumer(s -> {
            GenericRecord record = null;
            while(dataFileStream.hasNext()){
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
