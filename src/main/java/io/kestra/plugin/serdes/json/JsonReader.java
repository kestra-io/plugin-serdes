package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read a json file and write it to an ion serialized data file."
)
public class JsonReader extends Task implements RunnableTask<JsonReader.Output> {
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
    private final String charset = StandardCharsets.UTF_8.name();

    @Builder.Default
    @Schema(
        title = "Is the file is a json new line (JSON-NL)",
        description ="Is the file is a json with new line separator\n" +
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
    )
    private final Boolean newLine = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            Flowable<Object> flowable = Flowable
                .create(this.nextRow(input), BackpressureStrategy.BUFFER)
                .doOnNext(row -> FileSerde.write(output, row));

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();
            runContext.metric(Counter.of("records", lineCount));

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
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;
    }

    private FlowableOnSubscribe<Object> nextRow(BufferedReader inputStream) {
        ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module());

        return s -> {
            if (newLine) {
                String line;
                while ((line = inputStream.readLine()) != null) {
                    s.onNext(mapper.readValue(line, Object.class));
                }
            } else {
                Object objects = mapper.readValue(inputStream, Object.class);

                if (objects instanceof Collection) {
                    ((Collection<?>) objects)
                        .forEach(s::onNext);
                } else {
                    s.onNext(objects);
                }
            }

            s.onComplete();
        };
    }
}
