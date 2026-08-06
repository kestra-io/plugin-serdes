package io.kestra.plugin.serdes.csv;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.OnBadLines;
import io.kestra.plugin.serdes.OnEmptyHeader;

import de.siegmar.fastcsv.reader.CsvParseException;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a CSV file to the Amazon ION format",
    description = """
        Supports configurable field separator, text delimiter, charset, and \
        header detection. The value `\\N` is treated as null in any field. \
        Use `onBadLines` to control error handling for malformed rows. \
        A leading UTF-8 byte-order mark is stripped automatically. Trailing unnamed \
        header columns (e.g. from a trailing field separator) are dropped by default; \
        set `onEmptyHeader` to `RENAME` to keep them with generated names (col_0, col_1, ...) \
        instead."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a CSV file to the Amazon ION format.",
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
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.csv.CsvReader"
)
public class CsvToIon extends Task implements RunnableTask<CsvToIon.Output> {
    private static final int DEFAULT_MAX_BUFFER_SIZE = 16 * 1024 * 1024;
    private static final int DEFAULT_MAX_FIELD_SIZE = 16 * 1024 * 1024;

    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header"
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> header = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    @PluginProperty(group = "destination")
    private final Property<Character> fieldSeparator = Property.ofValue(',');

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    @PluginProperty(group = "processing")
    private final Property<Character> textDelimiter = Property.ofValue('"');

    @Builder.Default
    @Schema(
        title = "Specifies if empty rows should be skipped"
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> skipEmptyRows = Property.ofValue(false);

    @Schema(
        title = "Specifies if an exception should be thrown, if CSV data contains different field count"
    )
    @Deprecated
    @PluginProperty(group = "deprecated")
    private Property<Boolean> errorOnDifferentFieldCount;

    @Builder.Default
    @Schema(
        title = "How to handle bad lines (e.g., a line with too many fields)"
    )
    @PluginProperty(group = "advanced")
    private final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);

    @Builder.Default
    @Schema(
        title = "How to handle columns whose header name is empty",
        description = "`DROP` (default) removes trailing unnamed columns; `RENAME` keeps every column and names unnamed ones col_0, col_1, ... to avoid losing data."
    )
    @PluginProperty(group = "advanced")
    private final Property<OnEmptyHeader> onEmptyHeader = Property.ofValue(OnEmptyHeader.DROP);

    @Builder.Default
    @Schema(
        title = "Number of lines to skip at the start of the file"
    )
    @PluginProperty(group = "advanced")
    private final Property<Integer> skipRows = Property.ofValue(0);

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    @PluginProperty(group = "processing")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(title = "Maximum CSV parser buffer size (bytes)")
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxBufferSize = Property.ofValue(DEFAULT_MAX_BUFFER_SIZE);

    @Builder.Default
    @Schema(title = "Allow extra characters after a closing quote")
    @PluginProperty(group = "advanced")
    private final Property<Boolean> allowExtraCharsAfterClosingQuote = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Maximum field size (characters)")
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxFieldSize = Property.ofValue(DEFAULT_MAX_FIELD_SIZE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        AtomicInteger skipped = new AtomicInteger();
        Long lineCount = null;

        try (
            Reader reader = new BufferedReader(
                new InputStreamReader(
                    BOMInputStream.builder()
                        .setInputStream(runContext.storage().getFile(rFrom))
                        .setByteOrderMarks(ByteOrderMark.UTF_8)
                        .get(),
                    runContext.render(charset).as(String.class).orElseThrow()
                ),
                FileSerde.BUFFER_SIZE
            );
            CsvReader<CsvRecord> csvReader = this.csvReader(reader, runContext);
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var rHeaderValue = runContext.render(header).as(Boolean.class).orElseThrow();
            var rSkipRowsValue = runContext.render(this.skipRows).as(Integer.class).orElseThrow();
            Map<Integer, String> headers = new TreeMap<>();
            AtomicInteger effectiveHeaderCount = new AtomicInteger();
            OnBadLines rOnBadLinesValue = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);
            OnEmptyHeader rOnEmptyHeaderValue = runContext.render(this.onEmptyHeader).as(OnEmptyHeader.class).orElse(OnEmptyHeader.DROP);

            Flux<Object> flowable = Flux
                .fromIterable(csvReader)
                .onErrorResume(CsvParseException.class, e ->
                {
                    if (rOnBadLinesValue == OnBadLines.ERROR) {
                        return Flux.error(e);
                    } else if (rOnBadLinesValue == OnBadLines.WARN) {
                        runContext.logger().warn("Bad line encountered (skipped): {}", e.getMessage());
                    } else if (rOnBadLinesValue == OnBadLines.SKIP) {
                        // silently skip
                    }
                    return Flux.empty();
                })
                .filter(csvRecord ->
                {
                    if (rHeaderValue && csvRecord.getStartingLineNumber() == 1) {
                        effectiveHeaderCount.set(this.resolveHeaders(csvRecord, headers, rOnEmptyHeaderValue, runContext));
                        return false;
                    }
                    if (rSkipRowsValue > 0 && skipped.get() < rSkipRowsValue) {
                        skipped.incrementAndGet();
                        return false;
                    }
                    return true;
                })

                .flatMap(r ->
                {
                    if (rHeaderValue) {
                        Map<String, Object> fields = new LinkedHashMap<>();
                        if (r.getFieldCount() != headers.size()) {
                            String message = "Bad line encountered (field count mismatch): Expected "
                                + headers.size() + ", got " + r.getFieldCount() + " fields.";
                            if (rOnBadLinesValue == OnBadLines.ERROR) {
                                return Mono.error(new RuntimeException(message));
                            } else if (rOnBadLinesValue == OnBadLines.WARN) {
                                runContext.logger().warn(message);
                            }
                            return Mono.empty();
                        }
                        for (int i = 0; i < effectiveHeaderCount.get(); i++) {
                            String fieldValue = i < r.getFieldCount() ? r.getField(i) : null;
                            if ("\\N".equals(fieldValue)) {
                                fieldValue = null;
                            }
                            fields.put(headers.get(i), fieldValue);
                        }
                        return Mono.just(fields);
                    } else {
                        List<Object> fields = new ArrayList<>(r.getFieldCount());
                        for (int i = 0; i < r.getFieldCount(); i++) {
                            String fieldValue = r.getField(i);
                            if ("\\N".equals(fieldValue)) {
                                fieldValue = null;
                            }
                            fields.add(fieldValue);
                        }
                        return Mono.just(fields);
                    }
                });

            Mono<Long> count = FileSerde.writeAll(output, flowable);

            lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(lineCount != null ? lineCount : 0L)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;

        @Schema(title = "The number of records converted")
        private long size;
    }

    /**
     * Populates {@code headers} from the header record and returns the number of columns to emit,
     * applying the {@code onEmptyHeader} policy to columns whose name is empty.
     */
    private int resolveHeaders(CsvRecord header, Map<Integer, String> headers, OnEmptyHeader onEmptyHeader, RunContext runContext) {
        List<String> names = header.getFields();
        IntStream.range(0, names.size()).forEach(index -> headers.put(index, names.get(index)));

        if (onEmptyHeader == OnEmptyHeader.RENAME) {
            // Name every unnamed column uniquely, so no data is lost and Parquet gets valid names.
            Set<String> taken = new HashSet<>(names);
            headers.replaceAll((index, name) -> {
                if (!name.isEmpty()) {
                    return name;
                }
                String unique = IntStream.iterate(1, attempt -> attempt + 1)
                    .mapToObj(attempt -> attempt == 1 ? "col_" + index : "col_" + index + "_" + attempt)
                    .filter(candidate -> !taken.contains(candidate))
                    .findFirst()
                    .orElseThrow();
                taken.add(unique);
                return unique;
            });
            return names.size();
        }

        // DROP: drop the trailing run of unnamed columns (e.g. a trailing separator), but keep at
        // least one column so rows never collapse to an empty record.
        int kept = IntStream.range(0, names.size())
            .filter(index -> !names.get(index).isEmpty())
            .max()
            .orElse(names.size() - 1) + 1;

        if (kept < names.size()) {
            runContext.logger().warn(
                "Dropped {} trailing unnamed header column(s); their values are not emitted. Set onEmptyHeader=RENAME to keep them.",
                names.size() - kept
            );
        }
        return kept;
    }

    private CsvReader<CsvRecord> csvReader(Reader reader, RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = CsvReader.builder();

        runContext.render(textDelimiter).as(Character.class)
            .ifPresent(builder::quoteCharacter);

        runContext.render(fieldSeparator).as(Character.class)
            .ifPresent(builder::fieldSeparator);

        runContext.render(skipEmptyRows).as(Boolean.class)
            .ifPresent(builder::skipEmptyLines);

        builder.allowMissingFields(true);
        builder.allowExtraFields(true);

        runContext.render(allowExtraCharsAfterClosingQuote).as(Boolean.class)
            .ifPresent(builder::allowExtraCharsAfterClosingQuote);

        runContext.render(maxBufferSize).as(Integer.class)
            .ifPresent(builder::maxBufferSize);

        var handlerBuilder = de.siegmar.fastcsv.reader.CsvRecordHandler.builder();
        runContext.render(maxFieldSize).as(Integer.class)
            .ifPresent(handlerBuilder::maxFieldSize);

        var handler = handlerBuilder.build();
        return builder.build(handler, reader);
    }
}
