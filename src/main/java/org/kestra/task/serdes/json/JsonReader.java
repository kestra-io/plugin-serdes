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

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Read a json file and write it to a java serialized data file."
)
public class JsonReader extends Task implements RunnableTask<JsonReader.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI"
    )
    private String from;

    @Builder.Default
    @InputProperty(
        description = "The name of a supported charset",
        body = "Default value is UTF-8."
    )
    private String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from));
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".javas");
        ObjectMapper mapper = new ObjectMapper();
        int count = 0;

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from), charset));
            ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(tempFile))
        ) {
            String line;
            while ((line = input.readLine()) != null) {
                ObjectsSerde.write(output, mapper.readValue(line, Object.class));
                count++;
            }

            runContext.metric(Counter.of("records", count));

            return Output
                .builder()
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
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
