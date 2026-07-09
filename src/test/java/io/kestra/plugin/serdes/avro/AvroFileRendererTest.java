package io.kestra.plugin.serdes.avro;

import io.kestra.core.preview.FilePreview;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroFileRendererTest {
    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "Widget",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
          ]
        }""";

    @ParameterizedTest
    @CsvSource({ "0, false", "100, false", "101, true" })
    void testTruncatedByRecordCount(int recordCount, boolean truncated) throws Exception {
        InputStream is = avroInputStream(recordCount);

        AvroFileRenderer renderer = new AvroFileRenderer();
        FilePreview rendered = renderer.render("avro", is, Optional.empty(), 100);

        assertThat(rendered.isTruncated(), is(truncated));
    }

    @Test
    void testContent() throws Exception {
        InputStream is = avroInputStream(2);

        AvroFileRenderer renderer = new AvroFileRenderer();
        FilePreview rendered = renderer.render("avro", is, Optional.empty(), 100);

        assertThat(rendered.getType(), is(FilePreview.Type.LIST));
        assertThat(rendered.isTruncated(), is(false));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) rendered.getContent();
        assertThat(rows, hasSize(2));
        assertThat(rows.get(0).get("id"), is(0));
        assertThat(rows.get(0).get("name"), is("name0"));
    }

    @Test
    void testUnsupportedExtensionThrows() {
        AvroFileRenderer renderer = new AvroFileRenderer();
        InputStream is = new ByteArrayInputStream(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> renderer.render("csv", is, Optional.empty(), 10));
    }

    private InputStream avroInputStream(int recordCount) throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(schema, output);
            for (int i = 0; i < recordCount; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", i);
                record.put("name", "name" + i);
                writer.append(record);
            }
        }

        return new ByteArrayInputStream(output.toByteArray());
    }
}
