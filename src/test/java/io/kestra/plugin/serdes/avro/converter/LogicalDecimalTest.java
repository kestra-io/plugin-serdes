package io.kestra.plugin.serdes.avro.converter;

import java.math.BigDecimal;
import java.util.stream.Stream;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverterTest;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LogicalDecimalTest {
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of("12.82", new BigDecimal("12.82"), 4, 2),
            Arguments.of("12.8", new BigDecimal("12.80"), 4, 2),
            Arguments.of(12.8F, new BigDecimal("12.80"), 4, 2),
            Arguments.of("12.828282", new BigDecimal("12.828282"), 8, 6),
            Arguments.of(12L, new BigDecimal("12.00"), 4, 2),
            Arguments.of(12, new BigDecimal("12.00"), 4, 2),
            Arguments.of(7, new BigDecimal("7.00"), 12, 2),
            Arguments.of(-7, new BigDecimal("-7.00"), 12, 2),
            Arguments.of(250L, new BigDecimal("250.00"), 12, 2),
            Arguments.of(30_000_000_000L, new BigDecimal("30000000000.00"), 14, 2),
            Arguments.of(5, new BigDecimal("5.00"), 10, 2),
            Arguments.of(12.8444D, new BigDecimal("12.84"), 4, 2),
            Arguments.of(12.8444F, new BigDecimal("12.84"), 4, 2),
            Arguments.of("2019", new BigDecimal("2019"), 4, 0)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Object v, BigDecimal expected, Integer precision, Integer scale) throws Exception {
        Schema schema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
        AvroConverterTest.Utils.oneField(v, expected, schema, false);
    }

    @Test
    void shouldThrowWhenIntegerExceedsPrecision() {
        Schema schema = LogicalTypes.decimal(3, 2).addToSchema(Schema.create(Schema.Type.BYTES));
        // 12 -> 12.00 needs precision 4: the Avro encoder rejects it, not the converter
        assertThrows(
            AvroTypeException.class,
            () -> AvroConverterTest.Utils.oneField(12, new BigDecimal("12.00"), schema, false)
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
