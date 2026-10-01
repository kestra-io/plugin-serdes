package io.kestra.plugin.serdes.avro.converter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.plugin.serdes.avro.AvroConverterTest;

public class ComplexUnionTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("null", Arrays.asList(Schema.Type.NULL, Schema.Type.BOOLEAN), null),
            Arguments.of("null", Arrays.asList(Schema.Type.BOOLEAN, Schema.Type.NULL), null),
            Arguments.of("1", Arrays.asList(Schema.Type.INT, Schema.Type.NULL), 1),
            // #397 (maintainer direction): a string listed in nullValues resolves to NULL
            // whenever the union has a NULL branch, regardless of branch order.
            Arguments.of("n/a", Arrays.asList(Schema.Type.NULL, Schema.Type.STRING), null),
            Arguments.of("n/a", Arrays.asList(Schema.Type.STRING, Schema.Type.NULL), null),
            Arguments.of("null", Arrays.asList(Schema.Type.NULL, Schema.Type.STRING), null),
            Arguments.of("null", Arrays.asList(Schema.Type.STRING, Schema.Type.NULL), null),
            Arguments.of("", Arrays.asList(Schema.Type.NULL, Schema.Type.STRING), null),
            Arguments.of("", Arrays.asList(Schema.Type.STRING, Schema.Type.NULL), null),
            // Unions without a NULL branch are untouched: the literal string is preserved.
            Arguments.of("n/a", Arrays.asList(Schema.Type.STRING, Schema.Type.INT), new Utf8("n/a")),
            // Unions without a STRING branch keep the existing behavior,
            // a nullValues string still resolves to NULL in both declaration orders.
            Arguments.of("n/a", Arrays.asList(Schema.Type.NULL, Schema.Type.INT), null),
            Arguments.of("n/a", Arrays.asList(Schema.Type.INT, Schema.Type.NULL), null),
            // A real (non-string) null value must resolve to the NULL branch regardless of declaration order,
            // never fall through to STRING and get stringified into the literal "null".
            Arguments.of(null, Arrays.asList(Schema.Type.NULL, Schema.Type.STRING), null),
            Arguments.of(null, Arrays.asList(Schema.Type.STRING, Schema.Type.NULL), null)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, List<Schema.Type> schemas, Object expected) throws Exception {
        AvroConverterTest.Utils.oneField(
            v, expected, Schema.createUnion(
                schemas
                    .stream()
                    .map(Schema::create)
                    .collect(Collectors.toList())
            ), false
        );
    }
}
