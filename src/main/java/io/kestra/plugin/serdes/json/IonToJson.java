package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an ION file into a JSONL file.",
    description = "JSONL is the referrer for newline-delimited JSON."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Download a CSV file and convert it to a JSON format.",
            code = """
                id: ion_to_json
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.http_download.uri }}"

                  - id: to_json
                    type: io.kestra.plugin.serdes.json.IonToJson
                    from: "{{ outputs.convert.uri }}"
                """
        )
    },
    aliases = "io.kestra.plugin.serdes.json.JsonWriter"
)
public class IonToJson extends Task implements RunnableTask<IonToJson.Output> {
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

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    private final Property<String> timeZoneId = Property.ofValue(ZoneId.systemDefault().toString());

    @Override
    public Output run(RunContext runContext) throws Exception {
        String suffix = runContext.render(this.newLine).as(Boolean.class).orElseThrow() ? ".jsonl" : ".json";
        File tempFile = runContext.workingDir().createTempFile(suffix).toFile();
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        var renderedCharset = runContext.render(this.charset).as(String.class).orElseThrow();
        try (
            OutputStream outfile = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), renderedCharset), FileSerde.BUFFER_SIZE);
        ) {
            ObjectMapper mapper = new ObjectMapper()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setSerializationInclusion(JsonInclude.Include.ALWAYS)
                .setTimeZone(TimeZone.getTimeZone(ZoneId.of(runContext.render(this.timeZoneId).as(String.class).orElseThrow())))
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());
            SequenceWriter objectWriter = mapper.writerFor(Object.class).writeValues(outfile);

            if (runContext.render(this.newLine).as(Boolean.class).orElseThrow()) {
                Flux<Object> flowable = FileSerde.readAll(inputStream)
                    .doOnNext(throwConsumer(o -> {
                        objectWriter.write(o);
                        outfile.write("\n".getBytes());
                    }));

                // metrics & finalize
                Mono<Long> count = flowable.count();
                Long lineCount = count.block();
                runContext.metric(Counter.of("records", lineCount));

            } else {
                AtomicLong lineCount = new AtomicLong();

                List<Object> list = new ArrayList<>();
                FileSerde.reader(inputStream, throwConsumer(e -> {
                    list.add(e);
                    lineCount.incrementAndGet();
                }));
                objectWriter.write(list);
                runContext.metric(Counter.of("records", lineCount.get()));
            }

            outfile.flush();
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
}
