package org.kestra.task.serdes.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

public class RecordTest {
    static String json(String str) {
        return str.replace("'", "\"");
    }

    static final String SCHEMA_WITHOUT_DEFAULT = json("{"
        + "'type': 'record', 'name': 'toto', 'fields': ["
        + "{'name': 'id', 'type': 'int'},"
        + "{'name': 'label', 'type': 'string'}"
        + "]}");

    static final String SCHEMA_WITH_DEFAULT = json("{"
        + "'type': 'record', 'name': 'toto', 'fields': ["
        + "{'name': 'id', 'type': 'int'},"
        + "{'name': 'label', 'type': 'string', 'default':'foo'}"
        + "]}");

    static final String SCHEMA_WITHOUT_DEFAULT_NULLABLE = json("{"
        + "'type': 'record', 'name': 'toto', 'fields': ["
        + "{'name': 'id', 'type': 'int'},"
        + "{'name': 'label', 'type': ['null','string']}"
        + "]}");

    static final String SCHEMA_WITH_DEFAULT_NULLABLE = json("{"
        + "'type': 'record', 'name': 'toto', 'fields': ["
        + "{'name': 'id', 'type': 'int'},"
        + "{'name': 'label', 'type': ['string','null'], 'default': 'foo'}"
        + "]}");

    static Map<String, Object> recordFull = new HashMap<>();
    static Map<String, Object> recordNullValue = new HashMap<>();
    static Map<String, Object> recordMissingValue = new HashMap<>();


    static {
        recordFull.put("id", 1);
        recordFull.put("label", "foo");

        recordNullValue.put("id", 1);
        recordNullValue.put("label", null);

        recordMissingValue.put("id", 1);
    }

    public static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(SCHEMA_WITHOUT_DEFAULT, recordFull, recordFull, true),
            Arguments.of(SCHEMA_WITHOUT_DEFAULT, recordNullValue, recordFull, false),
            Arguments.of(SCHEMA_WITHOUT_DEFAULT, recordMissingValue, recordFull, false),

            Arguments.of(SCHEMA_WITH_DEFAULT, recordMissingValue, recordFull, true),
            Arguments.of(SCHEMA_WITH_DEFAULT, recordFull, recordFull, true),
            Arguments.of(SCHEMA_WITH_DEFAULT, recordNullValue, recordFull, false),

            Arguments.of(SCHEMA_WITHOUT_DEFAULT_NULLABLE, recordFull, recordFull, true),
            Arguments.of(SCHEMA_WITHOUT_DEFAULT_NULLABLE, recordNullValue, recordNullValue, true),
            Arguments.of(SCHEMA_WITHOUT_DEFAULT_NULLABLE, recordMissingValue, recordFull, false),

            Arguments.of(SCHEMA_WITH_DEFAULT_NULLABLE, recordFull, recordFull, true),
            Arguments.of(SCHEMA_WITH_DEFAULT_NULLABLE, recordNullValue, recordNullValue, true),
            Arguments.of(SCHEMA_WITH_DEFAULT_NULLABLE, recordMissingValue, recordFull, true)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void sourceWithoutDefaultOkTest(String schema,
                                    HashMap<String, Object> v,
                                    HashMap<String, Object> expected,
                                    boolean working) {
        Schema type = new Schema.Parser().parse(json(schema));
        if (working) {
            AvroConverterTest.Utils.testRecordOk(type, v, expected);
        } else {
            AvroConverterTest.Utils.testRecordKo(type, v, expected);
        }
    }

}
