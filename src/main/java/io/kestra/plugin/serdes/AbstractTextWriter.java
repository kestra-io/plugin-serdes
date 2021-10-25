package io.kestra.plugin.serdes;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.validations.DateFormat;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor

public abstract class AbstractTextWriter extends Task {
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
    private final String timeFormat = "HH:mm:ss[XXX]";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use for zoned datetime"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    private final String dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    @PluginProperty(dynamic = true)
    private final String timeZoneId = ZoneId.systemDefault().toString();

    private transient DateTimeFormatter dateFormatter;
    private transient DateTimeFormatter timeFormatter;
    private transient DateTimeFormatter dateTimeFormatter;
    private transient ZoneId zoneId;

    protected void init(RunContext runContext) throws IllegalVariableEvaluationException {
        dateFormatter = DateTimeFormatter.ofPattern(runContext.render(this.dateFormat));
        timeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.timeFormat));
        dateTimeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.dateTimeFormat));
        zoneId = ZoneId.of(runContext.render(this.timeZoneId));
    }

    protected String convert(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant || value instanceof Date) {
            return this.dateTimeFormatter.withZone(zoneId).withZone(ZoneOffset.UTC).format((TemporalAccessor) value);
        } else if (value instanceof OffsetDateTime || value instanceof ZonedDateTime) {
            return this.dateTimeFormatter.withZone(zoneId).format((TemporalAccessor) value);
        } else if (value instanceof LocalDate) {
            return this.dateFormatter.format((TemporalAccessor) value);
        } else if (value instanceof LocalTime) {
            return this.timeFormatter.withZone(zoneId).format((TemporalAccessor) value);
        } else if (value instanceof OffsetTime) {
            return this.timeFormatter.format((TemporalAccessor) value);
        } else {
            return value.toString();
        }
    }
}
