package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.serdes.serializers.ObjectsSerde;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Read a java serialized data file and write it to a new line delimited json file."
)
public class JsonWriter extends Task implements RunnableTask<JsonWriter.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI",
        dynamic = true
    )
    private String from;

    @Builder.Default
    @InputProperty(
        description = "The name of a supported charset",
        body = "Default value is UTF-8.",
        dynamic = false
    )
    private String charset = StandardCharsets.UTF_8.name();

    @Builder.Default
    @InputProperty(
        description = "Is the file is a json new line (JSON-NL)",
        body = {
            "Is the file is a json with new line separator",
            "Warning, if not, the whole file will loaded in memory and can lead to out of memory!"
        },
        dynamic = false
    )
    private boolean newLine = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".jsonl");
        ObjectMapper mapper = new ObjectMapper();
        URI from = new URI(runContext.render(this.from));
        AtomicInteger count = new AtomicInteger();

        try (
            BufferedWriter outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)));
            ObjectInputStream inputStream = new ObjectInputStream(runContext.uriToInputStream(from))
        ) {
            if (this.newLine) {
                ObjectsSerde.reader(inputStream, throwConsumer(row -> {
                    outfile.write(mapper.writeValueAsString(row) + "\n");
                    count.getAndIncrement();
                }));
            } else {
                List<Object> list = new ArrayList<>();

                ObjectsSerde.reader(inputStream, throwConsumer(row -> {
                    list.add(row);
                    count.getAndIncrement();
                }));

                outfile.write(mapper.writeValueAsString(list));
            }
        }

        runContext.metric(Counter.of("records", count.get()));

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
}
