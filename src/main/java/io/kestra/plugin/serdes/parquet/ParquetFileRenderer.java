package io.kestra.plugin.serdes.parquet;

import io.kestra.core.preview.FilePreview;
import io.kestra.core.preview.FileRenderer;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroDeserializer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayInputStream;
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
    title = "Parquet file renderer",
    description = """
        Preview Parquet files inside the Kestra UI. Each row is decoded using the \
        file's embedded Avro schema."""
)
public class ParquetFileRenderer implements FileRenderer {
    static {
        ParquetTools.handleLogger();
    }

    @Override
    public boolean supports(String extension) {
        return "parquet".equalsIgnoreCase(extension);
    }

    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        byte[] data = IOUtils.toByteArray(inputStream);
        InputFile inputFile = new InMemoryInputFile(data);

        List<Object> records = new ArrayList<>();
        boolean truncated;

        AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(inputFile)
            .disableCompatibility()
            .withDataModel(AvroConverter.genericData());

        try (ParquetReader<GenericRecord> parquetReader = readerBuilder.build()) {
            GenericRecord record;
            while (records.size() < maxRows && (record = parquetReader.read()) != null) {
                records.add(AvroDeserializer.recordDeserializer(record));
            }

            truncated = parquetReader.read() != null;
        }

        return FilePreview.builder()
            .content(records)
            .truncated(truncated)
            .extension(extension)
            .type(FilePreview.Type.LIST)
            .build();
    }

    private static class InMemoryInputFile implements InputFile {
        private final byte[] data;

        InMemoryInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            return new DelegatingSeekableInputStream(bais) {
                @Override
                public long getPos() {
                    return data.length - bais.available();
                }

                @Override
                public void seek(long newPos) {
                    bais.reset();
                    bais.skip(newPos);
                }
            };
        }
    }
}
