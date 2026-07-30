package io.kestra.plugin.serdes.avro;

import io.kestra.core.preview.FilePreview;
import io.kestra.core.preview.FileRenderer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Avro file renderer",
    description = """
        Preview Avro files inside the Kestra UI. Each record is decoded using its \
        embedded schema and rendered as a row."""
)
public class AvroFileRenderer implements FileRenderer {
    @Override
    public boolean supports(String extension) {
        return "avro".equalsIgnoreCase(extension);
    }

    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        List<Object> records = new ArrayList<>();
        boolean truncated;

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, datumReader)) {
            GenericRecord record = null;
            while (dataFileStream.hasNext() && records.size() < maxRows) {
                record = dataFileStream.next(record);
                records.add(AvroDeserializer.recordDeserializer(record));
            }

            truncated = dataFileStream.hasNext();
        }

        return FilePreview.builder()
            .content(records)
            .truncated(truncated)
            .extension(extension)
            .type(FilePreview.Type.LIST)
            .build();
    }
}
