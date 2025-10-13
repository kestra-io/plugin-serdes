package io.kestra.plugin.serdes.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.siegmar.fastcsv.reader.CsvRecord;
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
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import io.kestra.plugin.serdes.OnBadLines;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a CSV file into ION."
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
        title = "Source file URI")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header"
    )
    private final Property<Boolean> header = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    private final Property<Character> fieldSeparator = Property.ofValue(',');

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    private final Property<Character> textDelimiter = Property.ofValue('"');

    @Builder.Default
    @Schema(
        title = "Specifies if empty rows should be skipped"
    )
    private final Property<Boolean> skipEmptyRows = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "How to handle bad lines (e.g., a line with too many fields)."
    )
    private final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);

    @Builder.Default
    @Schema(
        title = "Number of lines to skip at the start of the file"
    )
    private final Property<Integer> skipRows = Property.ofValue(0);

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(title = "Maximum CSV parser buffer size (bytes)")
    private final Property<Integer> maxBufferSize = Property.ofValue(DEFAULT_MAX_BUFFER_SIZE);

    @Builder.Default
    @Schema(title = "Allow extra characters after a closing quote")
    private final Property<Boolean> allowExtraCharsAfterClosingQuote = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Maximum field size (characters)")
    private final Property<Integer> maxFieldSize = Property.ofValue(DEFAULT_MAX_FIELD_SIZE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // configuration
        AtomicInteger skipped = new AtomicInteger();

        try (
            Reader reader = new BufferedReader(
                new InputStreamReader(
                    runContext.storage().getFile(from),
                    runContext.render(charset).as(String.class).orElseThrow()
                ),
                FileSerde.BUFFER_SIZE);
            de.siegmar.fastcsv.reader.CsvReader<CsvRecord> csvReader = this.csvReader(reader, runContext);
            Writer output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var headerValue = runContext.render(header).as(Boolean.class).orElseThrow();
            var skipRowsValue = runContext.render(this.skipRows).as(Integer.class).orElseThrow();
            Map<Integer, String> headers = new TreeMap<>();
            OnBadLines onBadLinesValue = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);

            Flux<Object> flowable = Flux
                .fromIterable(csvReader)
                .onErrorResume(de.siegmar.fastcsv.reader.CsvParseException.class, e -> {
                    if (onBadLinesValue == OnBadLines.ERROR) {
                        return Flux.error(e);
                    } else if (onBadLinesValue == OnBadLines.WARN) {
                        runContext.logger().warn("Bad line encountered (skipped): {}", e.getMessage());
                    } else if (onBadLinesValue == OnBadLines.SKIP) {
                        // silently skip
                    }
                    return Flux.empty();
                })
                .filter(csvRecord -> {
                    if (headerValue && csvRecord.getStartingLineNumber() == 1) {
                        for (int i = 0; i < csvRecord.getFieldCount(); i++) {
                            headers.put(i, csvRecord.getField(i));
                        }
                        return false;
                    }
                    if (skipRowsValue > 0 && skipped.get() < skipRowsValue) {
                        skipped.incrementAndGet();
                        return false;
                    }
                    return true;
                })
                .flatMap(r -> {
                    if (headerValue && r.getFieldCount() != headers.size()) {
                        String message = "Bad line encountered (field count mismatch): Expected "
                            + headers.size() + ", got " + r.getFieldCount() + " fields.";

                        if (onBadLinesValue == OnBadLines.ERROR) {
                            return Mono.error(new RuntimeException(message));
                        } else if (onBadLinesValue == OnBadLines.WARN) {
                            runContext.logger().warn(message);
                        }
                        // SKIP or WARN → emit nothing
                        return Mono.empty();
                    }

                    // Normal row conversion
                    Map<String, Object> fields = new LinkedHashMap<>();
                    for (Map.Entry<Integer, String> header : headers.entrySet()) {
                        int i = header.getKey();
                        String fieldValue = i < r.getFieldCount() ? r.getField(i) : null;
                        if ("\\N".equals(fieldValue)) {
                            fieldValue = null;
                        }
                        fields.put(header.getValue(), fieldValue);
                    }
                    return Mono.just(fields);
                });

           Mono<Long> count = FileSerde.writeAll(output, flowable);

            // metrics & finalize
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

    private de.siegmar.fastcsv.reader.CsvReader<CsvRecord> csvReader(Reader reader, RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = de.siegmar.fastcsv.reader.CsvReader.builder();

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