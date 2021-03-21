package io.kestra.plugin.serdes.avro.converter;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverterTest;

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

    static Stream<Arguments> sourceTimestamp() {
        return Stream.of(
            Arguments.of(1577362391123L, LogicalTypes.timestampMillis(), LocalDateTime.parse("2019-12-26T12:13:11.123+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of(1577362391123456L, LogicalTypes.timestampMicros(), LocalDateTime.parse("2019-12-26T12:13:11.123456+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("1577362391123", LogicalTypes.timestampMillis(), LocalDateTime.parse("2019-12-26T12:13:11.123+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("1577362391123456", LogicalTypes.timestampMicros(), LocalDateTime.parse("2019-12-26T12:13:11.123456+01:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC))
        );
    }

    @ParameterizedTest
    @MethodSource("sourceTimestamp")
    void convertTimestamp(Object v, LogicalType logicalType, Instant expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, logicalType.addToSchema(Schema.create(Schema.Type.LONG)));
    }

    static Stream<Arguments> withFormat() {
        return Stream.of(
            Arguments.of("2019-12-26 12:13 +02", "yyyy-MM-dd' 'HH:mm' 'X", LocalDateTime.parse("2019-12-26T12:13+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2019-12-26 12:13:59 +02", "yyyy-MM-dd' 'HH:mm:ss' 'X", LocalDateTime.parse("2019-12-26T12:13:59+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)),
            Arguments.of("2020-04-30 08:00:00 +0200", "yyyy-MM-dd' 'HH:mm:ss' 'XXXX", LocalDateTime.parse("2020-04-30T08:00:00+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC))
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
