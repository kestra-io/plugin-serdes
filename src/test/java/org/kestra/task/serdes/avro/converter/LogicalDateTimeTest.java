package org.kestra.task.serdes.avro.converter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.kestra.task.serdes.avro.AvroConverter;
import org.kestra.task.serdes.avro.AvroConverterTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;


public class LogicalDateTimeTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("2019-12-26T12:13", LocalDateTime.parse("2019-12-26T12:13+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26T12:13:11", LocalDateTime.parse("2019-12-26T12:13:11+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26T12:13:11.123000", LocalDateTime.parse("2019-12-26T12:13:11.123000", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26T12:13:11+01:00", LocalDateTime.parse("2019-12-26T12:13:11+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26T12:13:11.123000+01:00", LocalDateTime.parse("2019-12-26T12:13:11.123000+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC))
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(CharSequence v, Instant expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        AvroConverterTest.Utils.oneField(v, expected, LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    }

    static Stream<Arguments> withFormat() {
        return Stream.of(
            Arguments.of("2019-12-26 12:13 +02", "yyyy-MM-dd' 'HH:mm' 'X", LocalDateTime.parse("2019-12-26T12:13+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26 12:13:59 +02", "yyyy-MM-dd' 'HH:mm:ss' 'X", LocalDateTime.parse("2019-12-26T12:13:59+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC))
        );
    }

    @ParameterizedTest
    @MethodSource("withFormat")
    void convertWithFormat(CharSequence v, String format, Instant expected) throws Exception {
        AvroConverter avroConverter = AvroConverter.builder()
            .datetimeFormat(format)
            .build();

        AvroConverterTest.Utils.oneField(avroConverter, v, expected, LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    }

    // FIXME if you add "null" to the list, you expect it to fail, but it surprisingly succeed
    static Stream<Arguments> failedSource() {
        return Stream.of(
            Arguments.of("12:26:2019")
        );
    }

    @ParameterizedTest
    @MethodSource("failedSource")
    void failed(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    }
}
