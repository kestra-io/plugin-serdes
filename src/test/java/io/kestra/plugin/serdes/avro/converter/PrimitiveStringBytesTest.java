package io.kestra.plugin.serdes.avro.converter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.plugin.serdes.avro.AvroConverterTest;

public class PrimitiveStringBytesTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("a", "a"),
            Arguments.of("true", "true"),
            Arguments.of(1, "1"),
            Arguments.of(42D, "42.0"),
            Arguments.of(42F, "42.0"),
            Arguments.of(42L, "42"),
            Arguments.of(42.0D, "42.0"),
            Arguments.of("", ""),
            // The literal user string "null" is a real value and must survive untouched -
            // only a Java null is rejected (see convertNullFailsOnNonNullableString).
            Arguments.of("null", "null")
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, String expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, new Utf8(expected.getBytes()), Schema.create(Schema.Type.STRING), false);
    }

    @ParameterizedTest
    @MethodSource("source")
    static void convertBytes(Object v, String expected) throws Exception {
        AvroConverterTest.Utils.oneField(
            v, ByteBuffer.wrap(new Utf8(expected.getBytes()).getBytes()), Schema.create(Schema.Type.BYTES),
            false
        );
    }

    // A null value into a non-nullable string/bytes field must be rejected, not stringified to the literal "null".
    @Test
    void convertNullFailsOnNonNullableString() {
        AvroConverterTest.Utils.oneFieldFailed(null, Schema.create(Schema.Type.STRING), false);
    }

    @Test
    void convertNullFailsOnNonNullableBytes() {
        AvroConverterTest.Utils.oneFieldFailed(null, Schema.create(Schema.Type.BYTES), false);
    }

    // ENUM and FIXED reach primitiveString through complexEnum / primitiveBytes, so they inherit the same guard.
    @Test
    void convertNullFailsOnNonNullableEnum() {
        AvroConverterTest.Utils.oneFieldFailed(
            null, Schema.createEnum("enumName", null, null, List.of("A", "B")), false
        );
    }

    @Test
    void convertNullFailsOnNonNullableFixed() {
        AvroConverterTest.Utils.oneFieldFailed(null, Schema.createFixed("fixedName", null, null, 3), false);
    }
}
