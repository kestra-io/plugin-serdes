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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
        // Same rationale as ParquetToIon: load these once at plugin registration time,
        // not on the request thread, to avoid /tmp write restrictions under Java Security.
        ParquetTools.handleLogger();
        ParquetTools.initSnappy();
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

        // AvroParquetReader needs random access to the file, so the stream must be materialized locally first.
        File tempFile = File.createTempFile("parquet-preview_", ".parquet");
        try {
            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                IOUtils.copyLarge(inputStream, outputStream);
            }

            List<Object> records = new ArrayList<>();
            boolean truncated;

            HadoopInputFile parquetInputFile = HadoopInputFile.fromPath(new Path(tempFile.getPath()), new Configuration());
            AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(parquetInputFile)
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
            tempFile.delete();
        }
    }
}
