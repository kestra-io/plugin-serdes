package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.writer.CsvAppender;
import io.kestra.core.validations.DateFormat;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
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
        title = "Specifies if the first line should be the header"
    )
    private final Boolean header = true;

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    private final Character fieldSeparator = ",".charAt(0);

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    private final Character textDelimiter = "\"".charAt(0);

    @Builder.Default
    @Schema(
        title = "The character used to separate rows"
    )
    private final Character[] lineDelimiter = ArrayUtils.toObject("\n".toCharArray());

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

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for date"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String dateFormat = "yyyy-MM-dd";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for time"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String timeFormat = "HH:mm:ss";


    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for zoned time"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String zonedTimeFormat = "HH:mm:ssXXX";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for datetime"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for zoned datetime"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String zonedDatetimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    @PluginProperty(dynamic = true)
    private final String timeZoneId = ZoneId.systemDefault().toString();

    private transient DateTimeFormatter dateFormatter;
    private transient DateTimeFormatter timeFormatter;
    private transient DateTimeFormatter zonedTimeFormatter;
    private transient DateTimeFormatter datetimeFormatter;
    private transient DateTimeFormatter zonedDatetimeFormatter;
    private transient ZoneId zoneId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.tempFile(".csv").toFile();

        // writer
        de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter();

        // reader
        URI from = new URI(runContext.render(this.from));

        // formatter
        dateFormatter = DateTimeFormatter.ofPattern(runContext.render(this.dateFormat));
        timeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.timeFormat));
        zonedTimeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.zonedTimeFormat));
        datetimeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.datetimeFormat));
        zonedDatetimeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.zonedDatetimeFormat));
        zoneId = ZoneId.of(runContext.render(this.timeZoneId));

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
                            List<Object> casted = (List<Object>) row;

                            if (header) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            for (final Object value : casted) {
                                csvAppender.appendField(value != null ? value.toString() : null);
                            }
                        } else if (row instanceof Map) {
                            Map<String, Object> casted = (Map<String, Object>) row;

                            if (!first) {
                                this.first = true;
                                if (header) {
                                    for (final String value : casted.keySet()) {
                                        csvAppender.appendField(value);
                                    }
                                    csvAppender.endLine();
                                }
                            }

                            for (final Object value : casted.values()) {
                                csvAppender.appendField(convert(value));
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


    private String convert(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant || value instanceof Date) {
            return this.datetimeFormatter.withZone(zoneId).format((TemporalAccessor) value);
        } else if (value instanceof OffsetDateTime || value instanceof ZonedDateTime) {
            return this.zonedDatetimeFormatter.format((TemporalAccessor) value);
        } else if (value instanceof LocalDate) {
            return this.dateFormatter.format((TemporalAccessor) value);
        } else if (value instanceof LocalTime) {
            return this.timeFormatter.format((TemporalAccessor) value);
        } else if (value instanceof OffsetTime) {
            return this.zonedTimeFormatter.format((TemporalAccessor) value);
        } else {
            return value.toString();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
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
