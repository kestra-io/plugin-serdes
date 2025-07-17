package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.kestra.core.models.annotations.Example;
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

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.TimeZone;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a JSON file into ION.",
    description = """
        We support one JSON dictionary/map per line as well as a JSON file in array format.

        Here is how a sample JSON file content might look like:
        ```
        {"product_id":"1","product_name":"streamline turn-key systems","product_category":"Electronics","brand":"gomez"},
        {"product_id":"2","product_name":"morph viral applications","product_category":"Household","brand":"wolfe"},
        {"product_id":"3","product_name":"expedite front-end schemas","product_category":"Household","brand":"davis-martinez"}
        ```

        Here is how a sample JSON file in array format might look:
        ```
        [
            {"product_id":"1","product_name":"streamline turn-key systems","product_category":"Electronics","brand":"gomez"},
            {"product_id":"2","product_name":"morph viral applications","product_category":"Household","brand":"wolfe"},
            {"product_id":"3","product_name":"expedite front-end schemas","product_category":"Household","brand":"davis-martinez"}
        ]
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a JSON file to the Amazon Ion format.",
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
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(
        title = "Is the file is a json new line (JSON-NL)",
        description = "Is the file is a json with new line separator\n" +
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
    )
    private final Property<Boolean> newLine = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        var renderedCharset = runContext.render(this.charset).as(String.class).orElseThrow();
        var renderedNewLine = runContext.render(this.newLine).as(Boolean.class).orElseThrow();

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), renderedCharset), FileSerde.BUFFER_SIZE);
            Writer writer = new BufferedWriter(new FileWriter(tempFile, Charset.forName(renderedCharset)), FileSerde.BUFFER_SIZE)
        ) {
            Flux<Object> flowable = Flux
                .create(this.nextRow(input, renderedNewLine), FluxSink.OverflowStrategy.BUFFER);
            Mono<Long> count = FileSerde.writeAll(writer, flowable);

            // metrics & finalize
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;
    }

    private Consumer<FluxSink<Object>> nextRow(BufferedReader inputStream, boolean newLine) throws IOException {
        ObjectReader objectReader = OBJECT_MAPPER.readerFor(Object.class);

        return throwConsumer(s -> {
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
