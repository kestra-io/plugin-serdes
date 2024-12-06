package io.kestra.plugin.serdes.avro.converter;

import io.kestra.plugin.serdes.avro.AvroConverterTest;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComplexUnionTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("null", Arrays.asList(Schema.Type.NULL, Schema.Type.BOOLEAN),  null),
            Arguments.of("null", Arrays.asList(Schema.Type.BOOLEAN, Schema.Type.NULL),  null),
            Arguments.of("1", Arrays.asList(Schema.Type.INT, Schema.Type.NULL), 1),
            Arguments.of("n/a", Arrays.asList(Schema.Type.NULL, Schema.Type.STRING), null),
            Arguments.of("n/a", Arrays.asList(Schema.Type.STRING, Schema.Type.NULL),  new Utf8("n/a"))
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, List<Schema.Type> schemas, Object expected) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, Schema.createUnion(schemas
            .stream()
            .map(Schema::create)
            .collect(Collectors.toList())
        ), false);
    }
}
