package io.kestra.plugin.serdes.avro.converter;

import io.kestra.plugin.serdes.avro.AvroConverterTest;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class InferTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("true", true, Schema.create(Schema.Type.BOOLEAN)),
            Arguments.of("", null, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))),
            Arguments.of("false", false, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)))
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, Object expected, Schema schema) throws Exception {
        AvroConverterTest.Utils.oneField(v, expected, schema, true);
    }
}
