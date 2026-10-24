package io.kestra.plugin.serdes;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.validations.DateFormat;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor

public abstract class AbstractTextWriter extends Task {
    @Builder.Default
    @Schema(
        title = "Format to use for date"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    private final String dateFormat = "yyyy-MM-dd";

    @Builder.Default
    @Schema(
        title = "Format to use for time"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    private final String timeFormat = "HH:mm:ss[XXX]";

    @Builder.Default
    @Schema(
        title = "Format to use for zoned datetime"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    private final String dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS[XXX]";

    @Builder.Default
    @Schema(
        title = "Timezone to use when no timezone can be parsed on the source"
    )
    @PluginProperty(group = "advanced")
    private final Property<String> timeZoneId = Property.ofValue(ZoneId.systemDefault().toString());

    protected Formatters formatters(RunContext runContext) throws IllegalVariableEvaluationException {
        var zoneId = ZoneId.of(runContext.render(this.timeZoneId).as(String.class).orElseThrow());
        var time = DateTimeFormatter.ofPattern(runContext.render(this.timeFormat));
        var dateTime = DateTimeFormatter.ofPattern(runContext.render(this.dateTimeFormat));

        // zones are bound once here, since withZone() allocates a new formatter on every call
        return new Formatters(
            DateTimeFormatter.ofPattern(runContext.render(this.dateFormat)),
            time,
            time.withZone(zoneId),
            dateTime.withZone(zoneId),
            dateTime.withZone(ZoneOffset.UTC)
        );
    }

    protected static String convert(Object value, Formatters formatters) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant || value instanceof Date || value instanceof LocalDateTime) {
            return formatters.utcDateTime().format((TemporalAccessor) value);
        } else if (value instanceof OffsetDateTime || value instanceof ZonedDateTime) {
            return formatters.zonedDateTime().format((TemporalAccessor) value);
        } else if (value instanceof LocalDate) {
            return formatters.date().format((TemporalAccessor) value);
        } else if (value instanceof LocalTime) {
            return formatters.zonedTime().format((TemporalAccessor) value);
        } else if (value instanceof OffsetTime) {
            return formatters.time().format((TemporalAccessor) value);
        } else {
            return value.toString();
        }
    }

    protected record Formatters(
        DateTimeFormatter date,
        DateTimeFormatter time,
        DateTimeFormatter zonedTime,
        DateTimeFormatter zonedDateTime,
        DateTimeFormatter utcDateTime) {
    }
}
