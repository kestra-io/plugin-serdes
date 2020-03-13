package org.kestra.task.serdes.avro.converter;

import org.apache.avro.Schema;
import org.kestra.task.serdes.avro.AvroConverterTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

public class PrimitiveBoolTest {
    static Stream<Arguments> validSource() {
        return Stream.of(
            Arguments.of("true", true),
            Arguments.of("True", true),
            Arguments.of("1", true),
            Arguments.of(1, true),
            Arguments.of(true, true),
            Arguments.of("False", false),
            Arguments.of("0", false),
            Arguments.of(0, false),
            Arguments.of(false, false)
            );
    }

    @ParameterizedTest
    @MethodSource("validSource")
    void convert(Object v, boolean expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, Schema.create(Schema.Type.BOOLEAN));
    }

    static Stream<Arguments> invalidSource() {
        return Stream.of(
            Arguments.of("toto"),
            Arguments.of(2L),
            Arguments.of('a'),
            Arguments.of((Object) null),
            Arguments.of(""),
            Arguments.of(new ArrayList<>()),
            Arguments.of(new HashMap<>())
        );
    }
    @ParameterizedTest
    @MethodSource("invalidSource")
    void invalidBoolTest(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, Schema.create(Schema.Type.BOOLEAN));
    }
}
