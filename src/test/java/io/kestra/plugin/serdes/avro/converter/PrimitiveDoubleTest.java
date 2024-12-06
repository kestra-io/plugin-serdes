package io.kestra.plugin.serdes.avro.converter;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverterTest;

import java.util.stream.Stream;

public class PrimitiveDoubleTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(-42D, -42D),
            Arguments.of("-42", -42D),
            Arguments.of(-42, -42D),
            Arguments.of(-42D, -42D),
            Arguments.of(42D, 42D),
            Arguments.of("42", 42D),
            Arguments.of(42, 42D),
            Arguments.of(42D, 42D)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, double expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, Schema.create(Schema.Type.DOUBLE), false);
    }

    static Stream<Arguments> separator() {
        return Stream.of(
            Arguments.of("-42.1", -42.1D, '.'),
            Arguments.of("42,1", 42.1D, ','),
            Arguments.of("42|1", 42.1D, '|')
        );
    }

    @ParameterizedTest
    @MethodSource("separator")
    void convertSeparator(Object v, double expected, Character separator) throws Exception {
        AvroConverterTest.Utils.oneField(
            AvroConverter.builder().decimalSeparator(separator).build(),
            v,
            expected,
            Schema.create(Schema.Type.DOUBLE)
        );
    }

    static Stream<Arguments> failedSource() {
        return Stream.of(
            Arguments.of("a"),
            Arguments.of(9223372036854775807L)
        );
    }

    @ParameterizedTest
    @MethodSource("failedSource")
    void failed(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, Schema.create(Schema.Type.DOUBLE), false);
    }
}
