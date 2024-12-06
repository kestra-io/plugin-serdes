package io.kestra.plugin.serdes.avro.converter;

import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverterTest;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Nested
public
class LogicalDateTest {
    private Schema schema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

    Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("2019-12-26", LocalDate.parse("2019-12-26", DateTimeFormatter.ISO_DATE)),
            Arguments.of("2011-12-03+01:00", LocalDate.parse("2011-12-03+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(LocalDate.parse("2011-12-03+01:00", DateTimeFormatter.ISO_DATE), LocalDate.parse("2011-12-03+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(ZonedDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME), LocalDate.parse("2019-12-26+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(ZonedDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(), LocalDate.parse("2019-12-26+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(ZonedDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME).toOffsetDateTime(), LocalDate.parse("2019-12-26+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(ZonedDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME).toLocalDate(), LocalDate.parse("2019-12-26+01:00", DateTimeFormatter.ISO_DATE)),
            Arguments.of(ZonedDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME).toLocalDateTime(), LocalDate.parse("2019-12-26+01:00", DateTimeFormatter.ISO_DATE))
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, LocalDate expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, schema, false);
    }

    static Stream<Arguments> withFormat() {
        return Stream.of(
            Arguments.of("28/5/20", "d/M/yy", LocalDate.of(2020, 5, 28))
        );
    }

    @ParameterizedTest
    @MethodSource("withFormat")
    void convertWithFormat(CharSequence v, String format, LocalDate expected) throws Exception {
        AvroConverter avroConverter = AvroConverter
            .builder()
            .dateFormat(format)
            .build();

        AvroConverterTest.Utils.oneField(avroConverter, v, expected, LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    }

    Stream<Arguments> failedSource() {
        return Stream.of(
            Arguments.of("12-26-2019"),
            Arguments.of("2019-12+0100")
        );
    }

    @ParameterizedTest
    @MethodSource("failedSource")
    void failed(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, schema, false);
    }
}
