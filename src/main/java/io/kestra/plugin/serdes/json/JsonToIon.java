package io.kestra.plugin.serdes.json;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.TimeZone;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a JSON file to the Amazon ION format",
    description = """
        Converts a JSON file to Amazon ION, Kestra's internal binary format used \
        for passing data between tasks.

        Two input formats are supported: one JSON object per line (JSONL), \
        or a JSON array. Examples of each:

        ```json
        {"id": 1, "name": "Widget A", "category": "Electronics"}
        {"id": 2, "name": "Widget B", "category": "Household"}
        {"id": 3, "name": "Widget C", "category": "Furniture"}
        ```

        ```json
        [
          {"id": 1, "name": "Widget A", "category": "Electronics"},
          {"id": 2, "name": "Widget B", "category": "Household"},
          {"id": 3, "name": "Widget C", "category": "Furniture"}
        ]
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a JSON file to the Amazon ION format.",
            code = """
                id: json_to_ion
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/json/products.json

                  - id: to_ion
                    type: io.kestra.plugin.serdes.json.JsonToIon
                    from: "{{ outputs.http_download.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.json.JsonReader"
)
public class JsonToIon extends Task implements RunnableTask<JsonToIon.Output> {
    private static final int BUFFER_SIZE = 32 * 1024;
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson().copy()
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .setSerializationInclusion(JsonInclude.Include.ALWAYS)
        .setTimeZone(TimeZone.getDefault());

    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty(group = "processing")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(
        title = "Whether the file is newline-delimited JSON (JSONL)",
        description = "Whether the file uses newline-delimited JSON.\n" +
            "Warning: if not, the whole file will be loaded into memory and can lead to out-of-memory errors."
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> newLine = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        Long lineCount = null;

        var renderedCharset = runContext.render(this.charset).as(String.class).orElseThrow();
        var renderedNewLine = runContext.render(this.newLine).as(Boolean.class).orElseThrow();

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), renderedCharset), FileSerde.BUFFER_SIZE);
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            Flux<Object> flowable = Flux
                .create(this.nextRow(input, renderedNewLine), FluxSink.OverflowStrategy.BUFFER);
            Mono<Long> count = FileSerde.writeAll(output, flowable);

            // metrics & finalize
            lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(lineCount != null ? lineCount : 0L)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;

        @Schema(title = "The number of records converted")
        private long size;
    }

    private Consumer<FluxSink<Object>> nextRow(BufferedReader inputStream, boolean newLine) throws IOException {
        ObjectReader objectReader = OBJECT_MAPPER.readerFor(Object.class);

        return throwConsumer(s ->
        {
            if (newLine) {
                String line;
                while ((line = inputStream.readLine()) != null) {
                    s.next(objectReader.readValue(line));
                }
            } else {
                Object objects = objectReader.readValue(inputStream);

                if (objects instanceof Collection) {
                    ((Collection<?>) objects)
                        .forEach(s::next);
                } else {
                    s.next(objects);
                }
            }

            s.complete();
        });
    }
}
