package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategy;
import io.kestra.plugin.serdes.AbstractTextWriter;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a csv file."
)
public class CsvWriter extends AbstractTextWriter implements RunnableTask<CsvWriter.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header"
    )
    private final Boolean header = true;

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    private final Character fieldSeparator = ',';

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    private final Character textDelimiter = '"';

    @Builder.Default
    @Schema(
        title = "The character used to separate rows"
    )
    private final String lineDelimiter = "\n";

    @Builder.Default
    @Schema(
        title = "Whether fields should always be delimited using the textDelimiter option."
    )
    private final Boolean alwaysDelimitText = false;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final String charset = StandardCharsets.UTF_8.name();


    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.tempFile(".csv").toFile();

        // reader
        URI from = new URI(runContext.render(this.from));

        // formatter
        this.init(runContext);

        try (
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
            Writer fileWriter = new FileWriter(tempFile, Charset.forName(this.charset));
            de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter(fileWriter)
        ) {
            Flowable<Object> flowable = Flowable
                .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void accept(Object row) throws Exception {
                        if (row instanceof List) {
                            List<Object> casted = (List<Object>) row;

                            if (header) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            var rows = casted.stream().map(field -> convert(field)).toList();
                            csvWriter.writeRow(rows);
                        } else if (row instanceof Map) {
                            Map<String, Object> casted = (Map<String, Object>) row;

                            if (!first) {
                                this.first = true;
                                if (header) {
                                    var rows = casted.keySet().stream().map(field -> convert(field)).toList();
                                    csvWriter.writeRow(rows);
                                }
                            }

                            var rows = casted.values().stream().map(field -> convert(field)).toList();
                            csvWriter.writeRow(rows);
                        }
                    }
                });

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();
            runContext.metric(Counter.of("records", lineCount));
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

    private de.siegmar.fastcsv.writer.CsvWriter csvWriter(Writer writer) {
        var builder = de.siegmar.fastcsv.writer.CsvWriter.builder();

        builder.quoteCharacter(this.textDelimiter);
        builder.fieldSeparator(this.fieldSeparator);
        builder.lineDelimiter(LineDelimiter.of(this.lineDelimiter));
        builder.quoteStrategy(this.alwaysDelimitText ? QuoteStrategy.ALWAYS : QuoteStrategy.REQUIRED);

        return builder.build(writer);
    }
}
