package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Single;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.FileSerde;

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
@Documentation(
    description = "Read a json file and write it to an ion serialized data file."
)
public class JsonReader extends Task implements RunnableTask<JsonReader.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI",
        dynamic = true
    )
    private String from;

    @Builder.Default
    @InputProperty(
        description = "The name of a supported charset",
        body = "Default value is UTF-8."
    )
    private String charset = StandardCharsets.UTF_8.name();

    @Builder.Default
    @InputProperty(
        description = "Is the file is a json new line (JSON-NL)",
        body = {
            "Is the file is a json with new line separator",
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
        }
    )
    private boolean newLine = true;

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
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "URI of a temporary result file"
        )
        private URI uri;
    }

    private FlowableOnSubscribe<Object> nextRow(BufferedReader inputStream) {
        ObjectMapper mapper = new ObjectMapper();

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
                        .forEach(value -> {
                            s.onNext(value);
                        });
                } else {
                    s.onNext(objects);
                }
            }

            s.onComplete();
        };
    }
}
