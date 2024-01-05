package io.kestra.plugin.serdes.csv;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
        title = "Specifies if empty rows should be skipped"
    )
    private final Boolean skipEmptyRows = false;

    @Builder.Default
    @Schema(
        title = "Specifies if an exception should be thrown, if CSV data contains different field count"
    )
    private final Boolean errorOnDifferentFieldCount = false;

    @Builder.Default
    @Schema(
        title = "Number of lines to skip at the start of the file"
    )
    private final Integer skipRows = 0;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        // configuration
        AtomicInteger skipped = new AtomicInteger();

        try (
            de.siegmar.fastcsv.reader.CsvReader csvReader = this.csvReader(new InputStreamReader(runContext.uriToInputStream(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            Map<Integer, String> headers = new TreeMap<>();
            Flux<Object> flowable = Flux
                .fromIterable(csvReader)
                .filter(csvRow -> {
                    if (header && csvRow.getOriginalLineNumber() == 1) {
                        for (int i = 0; i < csvRow.getFieldCount(); i++) {
                            headers.put(i, csvRow.getField(i));
                        }
                        return false;
                    }
                    if (this.skipRows > 0 && skipped.get() < this.skipRows) {
                        skipped.incrementAndGet();
                        return false;
                    }

                    return true;
                })
                .map(r -> {
                    if (header) {
                        Map<String, Object> fields = new LinkedHashMap<>();
                        for (Map.Entry<Integer, String> header : headers.entrySet()) {
                            fields.put(header.getValue(), r.getField(header.getKey()));
                        }
                        return fields;
                    }
                    return r.getFields();
                })
                .doOnNext(throwConsumer(row -> FileSerde.write(output, row)));

            // metrics & finalize
            Mono<Long> count = flowable.count();
            Long lineCount = count.block();
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

    private de.siegmar.fastcsv.reader.CsvReader csvReader(InputStreamReader inputStreamReader) {
        var builder = de.siegmar.fastcsv.reader.CsvReader.builder();

        if (this.textDelimiter != null) {
            builder.quoteCharacter(textDelimiter);
        }

        if (this.fieldSeparator != null) {
            builder.fieldSeparator(fieldSeparator);
        }

        if (this.skipEmptyRows != null) {
            builder.skipEmptyRows(skipEmptyRows);
        }

        if (this.errorOnDifferentFieldCount != null) {
            builder.errorOnDifferentFieldCount(errorOnDifferentFieldCount);
        }

        return builder.build(inputStreamReader);
    }
}
