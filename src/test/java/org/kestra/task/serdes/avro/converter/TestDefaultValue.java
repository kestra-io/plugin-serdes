package org.kestra.task.serdes.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kestra.task.serdes.avro.AvroConverter;
import org.kestra.task.serdes.avro.AvroConverterTest;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.kestra.task.serdes.avro.AvroConverterTest.Utils.oneFieldSchema;

public class TestDefaultValue {
    static String json(String str) {
        return str.replace("'", "\"");
    }

    static final String SCHEMA = json("{'name': 'root', 'type': 'record', 'fields': [{'name': 'id', 'type': ['null','int']}]}");

    static final Map<String, Object> empty = new HashMap<>();
    static final Map<String, Object> expected = new HashMap<>();

    static {expected.put("id", null);}

    @Test
    void testWithDefaultIsNull() {
        Schema schema = new Schema.Parser().parse(SCHEMA);
        AvroConverter avroConverter = AvroConverter.builder().missingDefaultIsNull(true).build();
        try {
            assertThat(avroConverter.fromMap(schema, empty), is(avroConverter.fromMap(schema, expected)));
        } catch (AvroConverter.IllegalRowConvertion e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testWithDefaultNotIsNull() {
        Schema schema = new Schema.Parser().parse(SCHEMA);
        AvroConverter avroConverter = AvroConverter.builder().missingDefaultIsNull(false).build();
        try {avroConverter.fromMap(schema, empty);} catch (Exception ignored) {}
        assertThrows(AvroConverter.IllegalRowConvertion.class, () -> avroConverter.fromMap(schema, empty));
    }
}