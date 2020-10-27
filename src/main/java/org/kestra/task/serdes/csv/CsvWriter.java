package org.kestra.task.serdes.csv;

import de.siegmar.fastcsv.writer.CsvAppender;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.FileSerde;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a csv file."
)
public class CsvWriter extends Task implements RunnableTask<CsvWriter.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header (default: false)"
    )
    private final Boolean header = true;

    @Builder.Default
    @Schema(
        title = "The field separator character (default: ',' - comma)"
    )
    private final Character fieldSeparator = ",".charAt(0);

    @Builder.Default
    @Schema(
        title = "The text delimiter character (default: '\"' - double quotes)"
    )
    private final Character textDelimiter = "\"".charAt(0);

    @Builder.Default
    @Schema(
        title = "The character used to separate rows"
    )
    private final Character[] lineDelimiter = ArrayUtils.toObject("\n".toCharArray());

    @Builder.Default
    @Schema(
        title = "Whether fields should always be delimited using the textDelimiter option.",
        description = "Default value is false"
    )
    private final Boolean alwaysDelimitText = false;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private final String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".csv");

        // writer
        de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter();

        // reader
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
            CsvAppender csvAppender = csvWriter.append(tempFile, Charset.forName(this.charset));
        ) {
            Flowable<Object> flowable = Flowable
                .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
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
                });

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();
            runContext.metric(Counter.of("records", lineCount));

            csvAppender.flush();
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
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
