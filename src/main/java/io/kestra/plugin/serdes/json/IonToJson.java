package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
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
    title = "Read an ion serialized data file and write it to a new line delimited json file."
)
@Plugin(
    examples = {
    @Example(
        full = true,
        title = "Download a CSV file and convert it to a JSON format.",
        code = """     
id: ion_to_json
namespace: dev

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
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty(dynamic = true)
    private final String charset = StandardCharsets.UTF_8.name();

    @Builder.Default
    @Schema(
        title = "Is the file is a json new line (JSON-NL)",
        description = "Is the file is a json with new line separator\n" +
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
    )
    @PluginProperty(dynamic = false)
    private final boolean newLine = true;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    @PluginProperty(dynamic = true)
    private final String timeZoneId = ZoneId.systemDefault().toString();

    @Override
    public Output run(RunContext runContext) throws Exception {
        String suffix = this.newLine ? ".jsonl" : ".json";
        File tempFile = runContext.tempFile(suffix).toFile();
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedWriter outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)));
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)));
        ) {
            ObjectMapper mapper = new ObjectMapper()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setSerializationInclusion(JsonInclude.Include.ALWAYS)
                .setTimeZone(TimeZone.getTimeZone(ZoneId.of(runContext.render(this.timeZoneId))))
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());

            if (this.newLine) {
                Flux<Object> flowable = Flux
                    .create(FileSerde.reader(inputStream), FluxSink.OverflowStrategy.BUFFER)
                    .doOnNext(throwConsumer(o -> outfile.write(mapper.writeValueAsString(o) + "\n")));

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
                outfile.write(mapper.writeValueAsString(list));
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
