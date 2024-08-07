package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.reader.CsvRecord;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a CSV file to the Amazon Ion format.",
            code = """     
id: csv_to_ion
namespace: company.team

tasks:
  - id: http_download
    type: io.kestra.plugin.core.http.Download
    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

  - id: to_ion
    type: io.kestra.plugin.serdes.csv.CsvToIon
    from: "{{ outputs.http_download.uri }}"
"""
        )
    },
    aliases = "io.kestra.plugin.serdes.csv.CsvReader"
)
public class CsvToIon extends Task implements RunnableTask<CsvToIon.Output> {
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
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // configuration
        AtomicInteger skipped = new AtomicInteger();

        try (
            de.siegmar.fastcsv.reader.CsvReader<CsvRecord> csvReader = this.csvReader(new InputStreamReader(runContext.storage().getFile(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            Map<Integer, String> headers = new TreeMap<>();
            Flux<Object> flowable = Flux
                .fromIterable(csvReader)
                .filter(csvRecord -> {
                    if (header && csvRecord.getStartingLineNumber() == 1) {
                        for (int i = 0; i < csvRecord.getFieldCount(); i++) {
                            headers.put(i, csvRecord.getField(i));
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
            .uri(runContext.storage().putFile(tempFile))
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

    private de.siegmar.fastcsv.reader.CsvReader<CsvRecord> csvReader(InputStreamReader inputStreamReader) {
        var builder = de.siegmar.fastcsv.reader.CsvReader.builder();

        if (this.textDelimiter != null) {
            builder.quoteCharacter(textDelimiter);
        }

        if (this.fieldSeparator != null) {
            builder.fieldSeparator(fieldSeparator);
        }

        if (this.skipEmptyRows != null) {
            builder.skipEmptyLines(skipEmptyRows);
        }

        if (this.errorOnDifferentFieldCount != null) {
            builder.ignoreDifferentFieldCount(!errorOnDifferentFieldCount);
        }

        return builder.ofCsvRecord(inputStreamReader);
    }
}
