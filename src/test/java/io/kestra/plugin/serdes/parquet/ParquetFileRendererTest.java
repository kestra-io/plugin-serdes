package io.kestra.plugin.serdes.parquet;

import io.kestra.core.preview.FilePreview;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParquetFileRendererTest {
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
        InputStream is = parquetInputStream(recordCount);

        ParquetFileRenderer renderer = new ParquetFileRenderer();
        FilePreview rendered = renderer.render("parquet", is, Optional.empty(), 100);

        assertThat(rendered.isTruncated(), is(truncated));
    }

    @Test
    void testContent() throws Exception {
        InputStream is = parquetInputStream(2);

        ParquetFileRenderer renderer = new ParquetFileRenderer();
        FilePreview rendered = renderer.render("parquet", is, Optional.empty(), 100);

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
        ParquetFileRenderer renderer = new ParquetFileRenderer();
        InputStream is = new ByteArrayInputStream(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> renderer.render("avro", is, Optional.empty(), 10));
    }

    private InputStream parquetInputStream(int recordCount) throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        File tempFile = File.createTempFile("parquet-file-renderer-test_", ".parquet");
        tempFile.deleteOnExit();

        HadoopOutputFile outputFile = HadoopOutputFile.fromPath(new Path(tempFile.getPath()), new Configuration());
        AvroParquetWriter.Builder<GenericData.Record> writerBuilder = AvroParquetWriter.<GenericData.Record>builder(outputFile)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withSchema(schema);

        try (ParquetWriter<GenericData.Record> writer = writerBuilder.build()) {
            for (int i = 0; i < recordCount; i++) {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("id", i);
                record.put("name", "name" + i);
                writer.write(record);
            }
        }

        return new FileInputStream(tempFile);
    }
}
