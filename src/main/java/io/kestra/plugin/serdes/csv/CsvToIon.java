package io.kestra.plugin.serdes.csv;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;

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
import de.siegmar.fastcsv.reader.CsvRecordHandler;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
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
    private static final int DEFAULT_MAX_RECORD_SIZE = 4 * DEFAULT_MAX_FIELD_SIZE;

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
        description = """
            `DROP` (default) removes trailing unnamed columns. \
            `RENAME` keeps every column and names unnamed ones col_0, col_1, ... to avoid losing data."""
    )
    @PluginProperty(group = "advanced")
    private final Property<OnEmptyHeader> onEmptyHeader = Property.ofValue(OnEmptyHeader.DROP);

    @Builder.Default
    @Schema(
        title = "Number of rows to skip at the start of the file, before the header row is read",
        description = """
            Rows are counted at the CSV-record level, not by raw physical line: a quoted field \
            spanning several physical lines counts as a single row. When `header` is `true`, \
            skipped rows are removed first and the header is then read from the next row; when \
            `header` is `false`, the same number of rows is removed from the start of the data. \
            A negative value is treated as `0`. Rows dropped by `onBadLines` (`WARN` or `SKIP`) \
            never reach the skip counter, so a malformed row inside the skip window does not \
            count against `skipRows`."""
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
    @Schema(
        title = "Maximum CSV parser buffer size (characters)",
        description = "The FastCSV parser's maximum internal buffer size. The value must be positive and cannot exceed "
            + "2,147,483,647 characters."
    )
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxBufferSize = Property.ofValue(DEFAULT_MAX_BUFFER_SIZE);

    @Builder.Default
    @Schema(title = "Allow extra characters after a closing quote")
    @PluginProperty(group = "advanced")
    private final Property<Boolean> allowExtraCharsAfterClosingQuote = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum field size (characters)",
        description = "The maximum number of characters allowed in a field after processing. The default is "
            + "16,777,216 characters. When `maxRecordSize` is not set, the effective maximum record size is "
            + "the greater of 67,108,864 characters and this value."
    )
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxFieldSize = Property.ofValue(DEFAULT_MAX_FIELD_SIZE);

    @Schema(
        title = "Maximum record size (characters)",
        description = "The maximum total number of characters allowed in a record after processing. When unset, "
            + "the effective limit is the greater of 67,108,864 characters and `maxFieldSize`. When set, this "
            + "value must be greater than or equal to `maxFieldSize`."
    )
    @PluginProperty(group = "advanced")
    private Property<@Min(1) Integer> maxRecordSize;

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        String rCharset = runContext.render(charset).as(String.class).orElseThrow();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        AtomicInteger skipped = new AtomicInteger();
        Long lineCount = null;

        try (
            Reader reader = new BufferedReader(
                new InputStreamReader(
                    stripBomIfUtf8(runContext.storage().getFile(rFrom), rCharset),
                    rCharset
                ),
                FileSerde.BUFFER_SIZE
            );
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var rHeaderValue = runContext.render(header).as(Boolean.class).orElseThrow();
            Map<Integer, String> headers = new TreeMap<>();
            AtomicInteger effectiveHeaderCount = new AtomicInteger();
            AtomicBoolean headerResolved = new AtomicBoolean();
            int rSkipRowsBound = Math.max(runContext.render(this.skipRows).as(Integer.class).orElse(0), 0);
            OnBadLines rOnBadLinesValue = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);
            OnEmptyHeader rOnEmptyHeaderValue = runContext.render(this.onEmptyHeader).as(OnEmptyHeader.class).orElse(OnEmptyHeader.DROP);

            // Lenient modes count skipRows themselves, so dropped bad lines are included.
            var csvReaderSkipRows = rOnBadLinesValue == OnBadLines.ERROR ? rSkipRowsBound : 0;
            Flux<CsvRecord> flowable;
            if (rOnBadLinesValue == OnBadLines.ERROR) {
                CsvReader<CsvRecord> csvReader = this.csvReader(reader, runContext);
                flowable = Flux.fromIterable(csvReader);
            } else {
                var lenient = new LenientRecordReader(
                    reader instanceof BufferedReader b ? b : new BufferedReader(reader),
                    this.csvReaderBuilder(runContext, true),
                    this.createCsvRecordHandler(runContext),
                    this.renderQuote(runContext),
                    this.renderSeparator(runContext),
                    runContext.render(allowExtraCharsAfterClosingQuote).as(Boolean.class).orElse(false),
                    runContext.render(skipEmptyRows).as(Boolean.class).orElse(false),
                    this.renderMaxFieldSize(runContext),
                    this.renderMaxRecordSize(runContext),
                    rSkipRowsBound,
                    rOnBadLinesValue == OnBadLines.WARN ? runContext.logger() : null
                );

                flowable = Flux.<CsvRecord> generate(sink ->
                {
                    try {
                        var next = lenient.next();
                        if (next == null) {
                            sink.complete();
                        } else {
                            sink.next(next);
                        }
                    } catch (Exception e) {
                        sink.error(e);
                    }
                });
            }

            Mono<Long> count = FileSerde.writeAll(
                output,
                flowable
                    .filter(csvRecord ->
                    {
                        if (skipped.get() < csvReaderSkipRows) {
                            skipped.incrementAndGet();
                            return false;
                        }
                        if (rHeaderValue && headerResolved.compareAndSet(false, true)) {
                            effectiveHeaderCount.set(this.resolveHeaders(csvRecord, headers, rOnEmptyHeaderValue, runContext));
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
                    })
            );

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
     * Wraps the source stream to strip a leading UTF-8 byte-order mark, but only when the file is
     * read as UTF-8. For any other charset those three bytes are real data, so they are left alone.
     */
    private static InputStream stripBomIfUtf8(InputStream in, String charsetName) throws IOException {
        if (!StandardCharsets.UTF_8.equals(Charset.forName(charsetName))) {
            return in;
        }
        return BOMInputStream.builder()
            .setInputStream(in)
            .setByteOrderMarks(ByteOrderMark.UTF_8)
            .get();
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
            headers.replaceAll((index, name) ->
            {
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

        // Any empty or duplicate name left among the kept columns maps to the same output key, so
        // only the last such column's value survives. Warn rather than lose data silently (this
        // covers an all-empty header, which cannot be trimmed without collapsing every row).
        long distinctKept = IntStream.range(0, kept).mapToObj(names::get).distinct().count();
        if (distinctKept < kept) {
            runContext.logger().warn(
                "Header has {} duplicate or empty column name(s) among the kept columns. Colliding columns share one output key and only the last value is kept. Set onEmptyHeader=RENAME to keep every column.",
                kept - distinctKept
            );
        }
        return kept;
    }

    private CsvReader.CsvReaderBuilder csvReaderBuilder(RunContext runContext, boolean strictQuotes) throws IllegalVariableEvaluationException {
        var builder = CsvReader.builder();

        builder.quoteCharacter(renderQuote(runContext));
        builder.fieldSeparator(renderSeparator(runContext));

        runContext.render(skipEmptyRows).as(Boolean.class)
            .ifPresent(builder::skipEmptyLines);

        builder.allowMissingFields(true);
        builder.allowExtraFields(true);
        if (strictQuotes) {
            builder.allowUnclosedQuote(false);
        }

        runContext.render(allowExtraCharsAfterClosingQuote).as(Boolean.class)
            .ifPresent(builder::allowExtraCharsAfterClosingQuote);

        runContext.render(maxBufferSize).as(Integer.class)
            .ifPresent(builder::maxBufferSize);

        return builder;
    }

    private CsvRecordHandler createCsvRecordHandler(RunContext runContext) throws IllegalVariableEvaluationException {
        var handlerBuilder = CsvRecordHandler.builder();
        var rMaxFieldSize = renderMaxFieldSize(runContext);
        var rMaxRecordSize = renderMaxRecordSize(runContext);

        if (rMaxRecordSize < rMaxFieldSize) {
            throw new IllegalArgumentException(
                "maxRecordSize (" + rMaxRecordSize + ") must be greater than or equal to maxFieldSize (" + rMaxFieldSize + ")."
            );
        }

        handlerBuilder.maxFieldSize(rMaxFieldSize);
        handlerBuilder.maxRecordSize(rMaxRecordSize);

        return handlerBuilder.build();
    }

    private char renderQuote(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(textDelimiter).as(Character.class).orElse('"');
    }

    private char renderSeparator(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(fieldSeparator).as(Character.class).orElse(',');
    }

    private int renderMaxFieldSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(maxFieldSize).as(Integer.class).orElse(DEFAULT_MAX_FIELD_SIZE);
    }

    private int renderMaxRecordSize(RunContext runContext) throws IllegalVariableEvaluationException {
        var defaultSize = Math.max(DEFAULT_MAX_RECORD_SIZE, renderMaxFieldSize(runContext));
        return maxRecordSize == null ? defaultSize : runContext.render(maxRecordSize).as(Integer.class).orElse(defaultSize);
    }

    /**
     * Reads CSV records for {@code onBadLines} SKIP and WARN. FastCSV does not advance after a parse error,
     * so records are cut here from bounded physical lines. A record continues on the next line only while a
     * quoted field is still open. A record that is malformed, never closes, or exceeds {@code maxFieldSize}
     * or {@code maxRecordSize} drops its first physical line only; the following lines are read again as new
     * records. Line terminators are kept as read, and every dropped line counts toward {@code skipRows}.
     */
    private static final class LenientRecordReader {
        private enum State {
            FIELD_START,
            UNQUOTED,
            QUOTED,
            AFTER_QUOTE,
            INVALID
        }

        private static final String OVERLONG = new String("");

        private final BufferedReader reader;
        private final CsvReader.CsvReaderBuilder builder;
        private final CsvRecordHandler handler;
        private final char quote;
        private final char separator;
        private final boolean allowExtraAfterQuote;
        private final boolean skipEmptyRows;
        private final int maxFieldSize;
        private final int maxRecordSize;
        private final int skipRows;
        private final Logger warnLogger;
        private final ArrayDeque<String> replay = new ArrayDeque<>();
        private int skipped;
        private int quoteStart;

        private LenientRecordReader(
            BufferedReader reader,
            CsvReader.CsvReaderBuilder builder,
            CsvRecordHandler handler,
            char quote,
            char separator,
            boolean allowExtraAfterQuote,
            boolean skipEmptyRows,
            int maxFieldSize,
            int maxRecordSize,
            int skipRows,
            Logger warnLogger) {
            this.reader = reader;
            this.builder = builder;
            this.handler = handler;
            this.quote = quote;
            this.separator = separator;
            this.allowExtraAfterQuote = allowExtraAfterQuote;
            this.skipEmptyRows = skipEmptyRows;
            this.maxFieldSize = maxFieldSize;
            this.maxRecordSize = maxRecordSize;
            this.skipRows = skipRows;
            this.warnLogger = warnLogger;
        }

        CsvRecord next() throws IOException {
            while (true) {
                var first = nextLine();
                if (first == null) {
                    return null;
                }
                if (first == OVERLONG) {
                    dropped("line exceeded maxRecordSize");
                    continue;
                }
                if (skipEmptyRows && contentEnd(first) == 0) {
                    continue;
                }

                var record = new StringBuilder(first);
                var starts = new ArrayList<Integer>();
                starts.add(0);
                quoteStart = 0;
                var state = scan(State.FIELD_START, record, 0, contentEnd(first));
                var failure = state == State.INVALID ? "unexpected character after closing quote" : null;

                while (failure == null && state == State.QUOTED) {
                    var line = nextLine();
                    if (line == null) {
                        failure = "unclosed quoted field";
                    } else if (line == OVERLONG) {
                        failure = "line exceeded maxRecordSize";
                    } else {
                        var from = record.length();
                        starts.add(from);
                        record.append(line);
                        if (record.length() > maxRecordSize) {
                            failure = "record exceeded maxRecordSize";
                        } else if (record.length() - quoteStart > maxFieldSize) {
                            failure = "quoted field exceeded maxFieldSize";
                        } else {
                            state = scan(state, record, from, from + contentEnd(line));
                            if (state == State.INVALID) {
                                failure = "unexpected character after closing quote";
                            }
                        }
                    }
                }

                if (failure == null) {
                    try {
                        var csvRecord = builder.ofSingleCsvRecord(handler, record.toString());
                        if (skipRow()) {
                            continue;
                        }
                        return csvRecord;
                    } catch (CsvParseException e) {
                        failure = e.getMessage();
                    }
                }

                dropped(failure);
                for (int i = starts.size() - 1; i >= 1; i--) {
                    var end = i + 1 < starts.size() ? starts.get(i + 1) : record.length();
                    replay.addFirst(record.substring(starts.get(i), end));
                }
            }
        }

        private boolean skipRow() {
            if (skipped < skipRows) {
                skipped++;
                return true;
            }
            return false;
        }

        private void dropped(String reason) {
            if (warnLogger != null) {
                warnLogger.warn("Bad line encountered (skipped): {}", reason);
            }
            skipRow();
        }

        /** Length of a physical line without its terminator. */
        private static int contentEnd(String line) {
            var end = line.length();
            if (end > 0 && line.charAt(end - 1) == '\n') {
                end--;
            }
            if (end > 0 && line.charAt(end - 1) == '\r') {
                end--;
            }
            return end;
        }

        private State scan(State start, CharSequence text, int from, int to) {
            var state = start;
            for (int i = from; i < to; i++) {
                var ch = text.charAt(i);
                switch (state) {
                    case FIELD_START -> {
                        if (ch == quote) {
                            state = State.QUOTED;
                            quoteStart = i;
                        } else if (ch != separator) {
                            state = State.UNQUOTED;
                        }
                    }
                    case UNQUOTED -> {
                        if (ch == separator) {
                            state = State.FIELD_START;
                        }
                    }
                    case QUOTED -> {
                        if (ch == quote) {
                            state = State.AFTER_QUOTE;
                        }
                    }
                    case AFTER_QUOTE -> {
                        if (ch == quote) {
                            state = State.QUOTED;
                        } else if (ch == separator) {
                            state = State.FIELD_START;
                        } else {
                            state = allowExtraAfterQuote ? State.UNQUOTED : State.INVALID;
                        }
                    }
                    case INVALID -> {
                        return state;
                    }
                }
            }
            return state;
        }

        /**
         * Returns the next physical line with its terminator, {@code null} at end of input, or
         * {@link #OVERLONG} when it exceeds the limit.
         */
        private String nextLine() throws IOException {
            if (!replay.isEmpty()) {
                return replay.pollFirst();
            }
            var line = new StringBuilder();
            var overlong = false;
            var any = false;
            int c;
            while ((c = reader.read()) != -1) {
                any = true;
                if (c == '\n') {
                    line.append('\n');
                    break;
                }
                if (c == '\r') {
                    line.append('\r');
                    reader.mark(1);
                    if (reader.read() == '\n') {
                        line.append('\n');
                    } else {
                        reader.reset();
                    }
                    break;
                }
                if (line.length() >= maxRecordSize) {
                    overlong = true;
                } else {
                    line.append((char) c);
                }
            }
            if (!any) {
                return null;
            }
            return overlong ? OVERLONG : line.toString();
        }
    }

    private CsvReader<CsvRecord> csvReader(Reader reader, RunContext runContext) throws IllegalVariableEvaluationException {
        return this.csvReaderBuilder(runContext, false).build(this.createCsvRecordHandler(runContext), reader);
    }
}
