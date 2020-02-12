package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.ObjectsSerde;

import javax.validation.constraints.NotNull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".jsonl");

        // writer
        BufferedWriter outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)));
        ObjectMapper mapper = new ObjectMapper();

        // reader
        URI from = new URI(runContext.render(this.from));
        ObjectInputStream inputStream = new ObjectInputStream(runContext.uriToInputStream(from));

        Flowable<Object> flowable = Flowable
            .create(ObjectsSerde.<String, String>reader(inputStream), BackpressureStrategy.BUFFER)
            .observeOn(Schedulers.io())
            .doOnNext(o -> outfile.write(mapper.writeValueAsString(o) + "\n"))
            .doOnComplete(() -> {
                outfile.close();
                inputStream.close();
            });


        // metrics & finalize
        Single<Long> count = flowable.count();
        Long lineCount = count.blockingGet();
        runContext.metric(Counter.of("records", lineCount));

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile).getUri())
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