package io.kestra.plugin.serdes.avro.converter;

import io.kestra.plugin.serdes.avro.AvroConverterTest;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class PrimitiveLongTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(-42, -42L),
            Arguments.of("-42", -42L),
            Arguments.of(42, 42L),
            Arguments.of("42", 42L),
            Arguments.of(9223372036854775807L, 9223372036854775807L)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, long expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, Schema.create(Schema.Type.LONG), false);
    }

    static Stream<Arguments> failedSource() {
        return Stream.of(
            Arguments.of("-42.2"),
            Arguments.of(42.2D),
            Arguments.of(42.2F),
            Arguments.of("a"),
            Arguments.of("9223372036854775808")
        );
    }

    @ParameterizedTest
    @MethodSource("failedSource")
    void failed(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, Schema.create(Schema.Type.LONG), false);
    }
}
