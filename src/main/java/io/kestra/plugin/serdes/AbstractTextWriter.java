package io.kestra.plugin.serdes;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    private final String dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS[XXX]";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    private final Property<String> timeZoneId = Property.of(ZoneId.systemDefault().toString());

    @Getter(AccessLevel.NONE)
    private transient DateTimeFormatter dateFormatter;

    @Getter(AccessLevel.NONE)
    private transient DateTimeFormatter timeFormatter;

    @Getter(AccessLevel.NONE)
    private transient DateTimeFormatter dateTimeFormatter;

    @Getter(AccessLevel.NONE)
    private transient ZoneId zoneId;

    protected void init(RunContext runContext) throws IllegalVariableEvaluationException {
        dateFormatter = DateTimeFormatter.ofPattern(runContext.render(this.dateFormat));
        timeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.timeFormat));
        dateTimeFormatter = DateTimeFormatter.ofPattern(runContext.render(this.dateTimeFormat));
        zoneId = ZoneId.of(runContext.render(this.timeZoneId).as(String.class).orElseThrow());
    }

    protected String convert(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant || value instanceof Date || value instanceof LocalDateTime) {
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
