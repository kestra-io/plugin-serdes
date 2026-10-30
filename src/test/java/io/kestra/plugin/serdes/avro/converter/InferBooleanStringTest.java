package io.kestra.plugin.serdes.avro.converter;

import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.plugin.serdes.avro.AvroConverterTest;

// #445
public class InferBooleanStringTest {
    static Stream<Arguments> stringValues() {
        return Stream.of(
            Arguments.of("yes"),
            Arguments.of("no"),
            Arguments.of("on"),
            Arguments.of("off"),
            Arguments.of("0"),
            Arguments.of("1"),
            Arguments.of("true"),
            Arguments.of("false")
        );
    }

    @ParameterizedTest
    @MethodSource("stringValues")
    void stringFieldKeepsOriginalValue(String value) throws Exception {
        AvroConverterTest.Utils.oneField(value, new Utf8(value), Schema.create(Schema.Type.STRING), true);
    }

    @ParameterizedTest
    @MethodSource("stringValues")
    void nullableStringFieldKeepsOriginalValue(String value) throws Exception {
        AvroConverterTest.Utils.oneField(
            value,
            new Utf8(value),
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
            true
        );
    }

    @Test
    void mixedUnionFieldConvertsBooleanWordsButKeepsOtherStrings() throws Exception {
        Schema mixed = Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.BOOLEAN),
            Schema.create(Schema.Type.STRING)
        );
        AvroConverterTest.Utils.oneField("yes", true, mixed, true);
        AvroConverterTest.Utils.oneField("maybe", new Utf8("maybe"), mixed, true);
    }

    static Stream<Arguments> booleanValues() {
        return Stream.of(
            Arguments.of("yes", true),
            Arguments.of("no", false),
            Arguments.of("on", true),
            Arguments.of("off", false),
            Arguments.of("1", true),
            Arguments.of("0", false)
        );
    }

    @ParameterizedTest
    @MethodSource("booleanValues")
    void booleanFieldStillConverts(String value, boolean expected) throws Exception {
        AvroConverterTest.Utils.oneField(value, expected, Schema.create(Schema.Type.BOOLEAN), true);
    }

    @ParameterizedTest
    @MethodSource("booleanValues")
    void nullableBooleanFieldStillConverts(String value, boolean expected) throws Exception {
        AvroConverterTest.Utils.oneField(
            value,
            expected,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)),
            true
        );
    }
}
