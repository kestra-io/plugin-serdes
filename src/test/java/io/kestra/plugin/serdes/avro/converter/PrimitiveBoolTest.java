package io.kestra.plugin.serdes.avro.converter;

import io.kestra.plugin.serdes.avro.AvroConverterTest;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class PrimitiveBoolTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("true", true),
            Arguments.of("True", true),
            Arguments.of("1", true),
            Arguments.of(1, true),
            Arguments.of(true, true),
            Arguments.of("False", false),
            Arguments.of("0", false),
            Arguments.of(0, false),
            Arguments.of("", false),
            Arguments.of(false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, boolean expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, Schema.create(Schema.Type.BOOLEAN), false);
    }
}
