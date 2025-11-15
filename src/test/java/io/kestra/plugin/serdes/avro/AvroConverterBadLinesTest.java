package io.kestra.plugin.serdes.avro;

import io.kestra.plugin.serdes.OnBadLines;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AvroConverterBadLinesTest {

    private final AvroConverter converter = AvroConverter.builder()
        .strictSchema(true)
        .onBadLines(OnBadLines.ERROR)
        .build();

    private Schema simpleSchema = SchemaBuilder.record("TestRecord").fields()
        .name("id").type().optional().stringType()  // Nullable string
        .name("age").type().intType().noDefault()  // Non-nullable int
        .endRecord();

    @Test
    void testValidMapConversion() throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
        Map<String, Object> data = Map.of("id", "123", "age", 30);
        var record = converter.fromMap(simpleSchema, data, OnBadLines.SKIP);
        assertNotNull(record);
        assertEquals("123", record.get("id").toString());
        assertEquals(30, record.get("age"));
    }

    @ParameterizedTest
    @EnumSource(OnBadLines.class)
    void testNonNullableNullWithOnBadLines(OnBadLines onBadLines) throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "123");
        data.put("age", null);

        if (onBadLines == OnBadLines.ERROR) {
            assertThrows(AvroConverter.IllegalRowConvertion.class, () -> converter.fromMap(simpleSchema, data, onBadLines));
        } else {
            var record = converter.fromMap(simpleSchema, data, onBadLines);
            assertNotNull(record);
            assertEquals("123", record.get("id").toString());
            assertNull(record.get("age"));
        }
    }

    @ParameterizedTest
    @EnumSource(OnBadLines.class)
    void testTypeMismatchWithOnBadLines(OnBadLines onBadLines) throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "123");
        data.put("age", "invalid");

        if (onBadLines == OnBadLines.ERROR) {
            assertThrows(AvroConverter.IllegalRowConvertion.class, () -> converter.fromMap(simpleSchema, data, onBadLines));
        } else {
            var record = converter.fromMap(simpleSchema, data, onBadLines);
            assertNotNull(record);
            assertEquals("123", record.get("id").toString());
            assertNull(record.get("age"));
        }
    }

    @Test
    void testStrictSchemaViolation() throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
        Schema strictSchema = SchemaBuilder.record("StrictTest").fields()
            .name("id").type().stringType().noDefault()
            .endRecord();

        Map<String, Object> extraData = new HashMap<>(Map.of("id", "123"));
        extraData.put("extra", "bad");  // Extra field

        assertThrows(AvroConverter.IllegalStrictRowConversion.class, () ->
            converter.fromMap(strictSchema, extraData, OnBadLines.ERROR));

        assertDoesNotThrow(() ->
            converter.fromMap(strictSchema, extraData, OnBadLines.WARN));
        var recordWarn = converter.fromMap(strictSchema, extraData, OnBadLines.SKIP);
        assertNotNull(recordWarn);
        assertEquals("123", recordWarn.get("id").toString());
    }

    @Test
    void testNestedConversionPropagation() throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
        // Schema with nested record
        Schema nestedSchema = SchemaBuilder.record("Nested").fields()
            .name("outer").type().stringType().noDefault()
            .name("inner").type(SchemaBuilder.record("Inner").fields()
                .name("badField").type().intType().noDefault()
                .endRecord()).noDefault()
            .endRecord();

        Map<String, Object> inner = new HashMap<>();
        inner.put("badField", null);  // Bad null in non-nullable
        Map<String, Object> nestedData = Map.of("outer", "test", "inner", inner);

        var record = converter.fromMap(nestedSchema, nestedData, OnBadLines.SKIP);
        assertNotNull(record);
        assertEquals("test", record.get("outer").toString());
        var innerRecord = (org.apache.avro.generic.GenericRecord) record.get("inner");
        assertNotNull(innerRecord);
        assertNull(innerRecord.get("badField"));
    }
}