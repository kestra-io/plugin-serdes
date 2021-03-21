package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvRow;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read a csv file and write it to an ion serialized data file."
)
public class CsvReader extends Task implements RunnableTask<CsvReader.Output> {
    @NotNull
    @Schema(
        title = "Source file URI")
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
        title = "Specifies if empty rows should be skipped (default: true)"
    )
    private final Boolean skipEmptyRows = false;

    @Builder.Default
    @Schema(
        title = "Specifies if an exception should be thrown, if CSV data contains different field count (default: false)"
    )
    private final Boolean errorOnDifferentFieldCount = false;

    @Builder.Default
    @Schema(
        title = "Number of lines to skip at the start of the file"
    )
    private final Integer skipRows = 0;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private final String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));
        de.siegmar.fastcsv.reader.CsvReader csvReader = this.csvReader();

        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");

        // configuration
        AtomicInteger skipped = new AtomicInteger();

        try (
            CsvParser csvParser = csvReader.parse(new InputStreamReader(runContext.uriToInputStream(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            Flowable<Object> flowable = Flowable
                .create(this.nextRow(csvParser), BackpressureStrategy.BUFFER)
                .filter(csvRow -> {
                    if (this.skipRows > 0 && skipped.get() < this.skipRows) {
                        skipped.incrementAndGet();
                        return false;
                    }

                    return true;
                })
                .map(r -> {
                    if (header) {
                        return r.getFieldMap();
                    } else {
                        return r.getFields();
                    }
                })
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }

    private FlowableOnSubscribe<CsvRow> nextRow(CsvParser csvParser) {
        return s -> {
            CsvRow row;
            while ((row = csvParser.nextRow()) != null) {
                s.onNext(row);
            }

            s.onComplete();
        };
    }

    private de.siegmar.fastcsv.reader.CsvReader csvReader() {
        de.siegmar.fastcsv.reader.CsvReader csvReader = new de.siegmar.fastcsv.reader.CsvReader();

        if (this.header != null) {
            csvReader.setContainsHeader(this.header);
        }

        if (this.textDelimiter != null) {
            csvReader.setTextDelimiter(textDelimiter);
        }

        if (this.fieldSeparator != null) {
            csvReader.setFieldSeparator(fieldSeparator);
        }

        if (this.skipEmptyRows != null) {
            csvReader.setSkipEmptyRows(skipEmptyRows);
        }

        if (this.errorOnDifferentFieldCount != null) {
            csvReader.setErrorOnDifferentFieldCount(errorOnDifferentFieldCount);
        }

        return csvReader;
    }
}
