package org.kestra.task.serdes.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kestra.task.serdes.avro.AvroConverterTest;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class PrimitiveStringBytesTest {
    static Stream<Arguments> validSource() {
        return Stream.of(
            Arguments.of("a", "a"),
            Arguments.of("true", "true"),
            Arguments.of(1, "1"),
            Arguments.of(42D, "42.0"),
            Arguments.of(42F, "42.0"),
            Arguments.of(42L, "42"),
            Arguments.of(42.0D, "42.0"),
            Arguments.of("", "")
        );
    }

    @ParameterizedTest
    @MethodSource("validSource")
    void convert(Object v, String expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, new Utf8(expected.getBytes()), Schema.create(Schema.Type.STRING));
    }

    @ParameterizedTest
    @MethodSource("validSource")
    void convertBytes(Object v, String expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, ByteBuffer.wrap(new Utf8(expected.getBytes()).getBytes()), Schema.create(Schema.Type.BYTES));
    }

    static Stream<Arguments> invalidSource() {
        return Stream.of(
            Arguments.of((Object) null)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSource")
    void invalidStringTest(Object v) {
        AvroConverterTest.Utils.oneFieldFailed(v, Schema.create(Schema.Type.STRING));
    }
}
