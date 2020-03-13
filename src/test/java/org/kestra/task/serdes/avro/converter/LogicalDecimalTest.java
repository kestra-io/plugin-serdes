package org.kestra.task.serdes.avro.converter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kestra.task.serdes.avro.AvroConverter;
import org.kestra.task.serdes.avro.AvroConverterTest;

import java.math.BigDecimal;
import java.util.stream.Stream;

public class LogicalDecimalTest {
    static Stream<Arguments> validSource() {
        return Stream.of(
            Arguments.of("1", new BigDecimal("1"), 1, 0),
            Arguments.of("1", new BigDecimal("1"), 100, 0),
            Arguments.of("12.82", new BigDecimal("12.82"), 4, 2),
            Arguments.of("12.8", new BigDecimal("12.80"), 4, 2),
            Arguments.of(12.8F, new BigDecimal("12.80"), 4, 2),
            Arguments.of("12.828282", new BigDecimal("12.828282"), 8, 6),
            Arguments.of("12", new BigDecimal("12.00"), 4, 2),
            Arguments.of("12.000000", new BigDecimal("12.00"), 4, 2),
            Arguments.of(12L, new BigDecimal("12.00"), 4, 2),
            Arguments.of(12, new BigDecimal("12.00"), 4, 2),
            Arguments.of(12.84D, new BigDecimal("12.84"), 4, 2),
            Arguments.of(12.84F, new BigDecimal("12.84"), 4, 2),
            Arguments.of("2019", new BigDecimal("2019"), 4, 0),
            Arguments.of(2019, new BigDecimal("2019"), 4, 0)
        );
    }

    static Stream<Arguments> invalidSource() {
        return Stream.of(
            Arguments.of(null, 4, 2),
            Arguments.of("tata", 4, 2),
            Arguments.of("", 4, 2),
            Arguments.of(100, 2, 0),
            Arguments.of("100", 2, 0),
            Arguments.of("111.11", 4, 1),
            Arguments.of("0.01", 4, 1)
        );
    }

    static Stream<Arguments> separator() {
        return Stream.of(
            Arguments.of("12.82", new BigDecimal("12.82"), 4, 2, '.'),
            Arguments.of("12,82", new BigDecimal("12.82"), 4, 2, ','),
            Arguments.of("12|82", new BigDecimal("12.82"), 4, 2, '|')
        );
    }

    @ParameterizedTest
    @MethodSource("validSource")
    void convert(Object v, BigDecimal expected, Integer precision, Integer scale) throws Exception {
        Schema schema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
        AvroConverterTest.Utils.oneField(v, expected, schema);
    }

    @ParameterizedTest
    @MethodSource("invalidSource")
    void testFail(Object v, Integer precision, Integer scale) {
        Schema schema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
        AvroConverterTest.Utils.oneFieldFailed(v, schema);
    }

    @ParameterizedTest
    @MethodSource("separator")
    void convertSeparator(Object v, BigDecimal expected, Integer precision, Integer scale, Character separator) throws Exception {
        AvroConverterTest.Utils.oneField(
            AvroConverter.builder().decimalSeparator(separator).build(),
            v,
            expected,
            LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))
        );
    }

}
