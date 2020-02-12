package org.kestra.task.serdes.csv;

import de.siegmar.fastcsv.writer.CsvAppender;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.ObjectsSerde;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.ObjectInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Read a java serialized data file and write it to a csv file."
)
public class CsvWriter extends Task implements RunnableTask<CsvWriter.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI"
    )
    private String from;

    @Builder.Default
    @InputProperty(
        description = "Specifies if the first line should be the header (default: false)"
    )
    private Boolean header = true;

    @Builder.Default
    @InputProperty(
        description = "The field separator character (default: ',' - comma)"
    )
    private Character fieldSeparator = ",".charAt(0);

    @Builder.Default
    @InputProperty(
        description = "The text delimiter character (default: '\"' - double quotes)"
    )
    private Character textDelimiter = "\"".charAt(0);

    @Builder.Default
    @InputProperty(
        description = "The character used to separate rows"
    )
    private Character[] lineDelimiter = ArrayUtils.toObject("\n".toCharArray());

    @Builder.Default
    @InputProperty(
        description = "Whether fields should always be delimited using the textDelimiter option.",
        body = "Default value is false"
    )
    private Boolean alwaysDelimitText = false;

    @Builder.Default
    @InputProperty(
        description = "The name of a supported charset",
        body = "Default value is UTF-8."
    )
    private String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".csv");

        // writer
        de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter();
        CsvAppender csvAppender = csvWriter.append(tempFile, Charset.forName(this.charset));

        // reader
        URI from = new URI(runContext.render(this.from));
        ObjectInputStream inputStream = new ObjectInputStream(runContext.uriToInputStream(from));

        Flowable<Object> flowable = Flowable
            .create(ObjectsSerde.<String, String>reader(inputStream), BackpressureStrategy.BUFFER)
            .observeOn(Schedulers.io())
            .doOnNext(new Consumer<>() {
                private boolean first = false;

                @SuppressWarnings("unchecked")
                @Override
                public void accept(Object row) throws Exception {
                    if (row instanceof List) {
                        List<String> casted = (List<String>) row;

                        if (header) {
                            throw new IllegalArgumentException("Invalid data of type List with header");
                        }

                        for (final String value : casted) {
                            csvAppender.appendField(value);
                        }
                    } else if (row instanceof Map) {
                        Map<String, String> casted = (Map<String, String>) row;

                        if (!first) {
                            this.first = true;
                            if (header) {
                                for (final String value : casted.keySet()) {
                                    csvAppender.appendField(value);
                                }
                                csvAppender.endLine();
                            }
                        }

                        for (final String value : casted.values()) {
                            csvAppender.appendField(value);
                        }
                    }

                    csvAppender.endLine();
                }
            })
            .doOnComplete(() -> {
                csvAppender.close();
                inputStream.close();
            });


        // metrics & finalize
        Single<Long> count = flowable.count();
        Long lineCount = count.blockingGet();
        runContext.metric(Counter.of("records", lineCount));

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

    private de.siegmar.fastcsv.writer.CsvWriter csvWriter() {
        de.siegmar.fastcsv.writer.CsvWriter csvWriter = new de.siegmar.fastcsv.writer.CsvWriter();

        csvWriter.setTextDelimiter(this.textDelimiter);
        csvWriter.setFieldSeparator(this.fieldSeparator);
        csvWriter.setLineDelimiter(ArrayUtils.toPrimitive(this.lineDelimiter));
        csvWriter.setAlwaysDelimitText(this.alwaysDelimitText);

        return csvWriter;
    }
}