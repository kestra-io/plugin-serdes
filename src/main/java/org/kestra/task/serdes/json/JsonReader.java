package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.ObjectsSerde;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

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
        // reader
        URI from = new URI(runContext.render(this.from));
        BufferedReader input = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from), charset));

        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".javas");
        ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(tempFile));

        // convert
        Flowable<Object> flowable = Flowable
            .create(this.nextRow(input), BackpressureStrategy.BUFFER)
            .observeOn(Schedulers.io())
            .doOnNext(row -> ObjectsSerde.write(output, row))
            .doOnComplete(() -> {
                output.close();
                input.close();
            });

        // metrics & finalize
        Single<Long> count = flowable.count();
        Long lineCount = count.blockingGet();

        return Output
            .builder()
            .uri(runContext.putFile(tempFile).getUri())
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
            String line;
            while ((line = inputStream.readLine()) != null) {
                s.onNext(mapper.readValue(line, Object.class));
            }

            s.onComplete();
        };
    }
}