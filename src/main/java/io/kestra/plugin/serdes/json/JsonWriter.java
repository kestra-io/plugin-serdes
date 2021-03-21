package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a new line delimited json file."
)
public class JsonWriter extends Task implements RunnableTask<JsonWriter.Output> {
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
    private String charset = StandardCharsets.UTF_8.name();

    @Builder.Default
    @Schema(
        title = "Is the file is a json new line (JSON-NL)",
        description = "Is the file is a json with new line separator\n" +
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
    )
    @PluginProperty(dynamic = false)
    private boolean newLine = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".jsonl");
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedWriter outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)));
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
        ) {
            ObjectMapper mapper = new ObjectMapper();

            if (this.newLine) {
                Flowable<Object> flowable = Flowable
                    .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
                    .doOnNext(o -> {
                        outfile.write(mapper.writeValueAsString(o) + "\n");
                    });

                // metrics & finalize
                Single<Long> count = flowable.count();
                Long lineCount = count.blockingGet();
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
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }
}
