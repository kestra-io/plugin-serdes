package io.kestra.plugin.serdes.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import io.kestra.core.models.annotations.Plugin;
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
@Plugin
public class ParquetFileRenderer implements FileRenderer {
    static {
        ParquetTools.handleLogger();
    }

    @Override
    public boolean supports(String extension) {
        return "parquet".equalsIgnoreCase(extension);
    }

    // No @Override: this branch's kestraVersion predates FileRenderer.extensions()
    // (kestra-io/kestra#16054). Overrides correctly once that dependency updates.
    public Set<String> extensions() {
        return Set.of("parquet");
    }

    // Parquet needs random access (footer at end, seeks to row groups), so it cannot be read from a
    // forward-only stream. Copy to a local temp file once, then seek-read, same as ParquetToIon.
    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        Path tmp = Files.createTempFile("parquet-preview_", ".parquet");
        try {
            try (InputStream in = inputStream) {
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            }

            InputFile inputFile = new LocalInputFile(tmp);

            List<Object> records = new ArrayList<>();
            boolean truncated;

            AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord> builder(inputFile)
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
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
