package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
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
public class JsonReader extends Task implements RunnableTask {
    @NotNull
    private String from;

    @Builder.Default
    private String charset = StandardCharsets.UTF_8.name();

    @Override
    public RunOutput run(RunContext runContext) throws Exception {
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

        return RunOutput.builder()
            .outputs(ImmutableMap.of("uri", runContext.putFile(tempFile).getUri()))
            .build();
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