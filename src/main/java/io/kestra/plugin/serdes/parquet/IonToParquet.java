package io.kestra.plugin.serdes.parquet;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.serdes.avro.AbstractAvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverter;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.Locale;
import jakarta.validation.constraints.NotNull;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided file containing ion serialized data and convert it to parquet."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Read a CSV file, transform it and store the transformed data as a parquet file.",
            code = """     
id: ion_to_parquet
namespace: dev

tasks:
  - id: download_csv
    type: io.kestra.plugin.core.http.Download
    description: salaries of data professionals from 2020 to 2023 (source ai-jobs.net)
    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/salaries.csv

  - id: avg_salary_by_job_title
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles:
      data.csv: "{{ outputs.download_csv.uri }}"
    sql: |
      SELECT 
        job_title,
        ROUND(AVG(salary),2) AS avg_salary
      FROM read_csv_auto('{{ workingDir }}/data.csv', header=True)
      GROUP BY job_title
      HAVING COUNT(job_title) > 10
      ORDER BY avg_salary DESC;
    store: true

  - id: result
    type: io.kestra.plugin.serdes.parquet.IonToParquet
    from: "{{ outputs.avg_salary_by_job_title.uri }}"
    schema: |
      {
        "type": "record",
        "name": "Salary",
        "namespace": "com.example.salary",
        "fields": [
          {"name": "job_title", "type": "string"},
          {"name": "avg_salary", "type": "double"}
        ]
      }
"""
        )
    },
    aliases = "io.kestra.plugin.serdes.parquet.ParquetWriter"
)
public class IonToParquet extends AbstractAvroConverter implements RunnableTask<IonToParquet.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The compression to used"
    )
    @PluginProperty(dynamic = false)
    CompressionCodec compressionCodec = CompressionCodec.GZIP;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target row group size"
    )
    @PluginProperty(dynamic = false)
    private Version version = Version.V2;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target row group size"
    )
    @PluginProperty(dynamic = false)
    private Long rowGroupSize = (long) org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target page size"
    )
    @PluginProperty(dynamic = false)
    private Integer pageSize = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Max dictionary page size"
    )
    @PluginProperty(dynamic = false)
    private Integer dictionaryPageSize = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

    static {
        ParquetTools.handleLogger();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file, we create multiple useless tree to avoid incompatibility with EE javaSecurity
        java.nio.file.Path tempDir = runContext.tempDir().resolve(IdUtils.create());
        tempDir.toFile().mkdirs();
        File tempFile = Files.createTempFile(tempDir, "", ".parquet").toFile();

        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro options
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        // reader
        URI from = new URI(runContext.render(this.from));

        // parquet options
        CompressionCodecName codec = this.compressionCodec.parquetCodec();
        HadoopOutputFile outfileFile = HadoopOutputFile.fromPath(new Path(tempFile.getPath()), new Configuration());

        AvroParquetWriter.Builder<GenericData.Record> parquetWriterBuilder = AvroParquetWriter
            .<GenericData.Record>builder(outfileFile)
            .withWriterVersion(version == Version.V2 ? PARQUET_2_0 : PARQUET_1_0)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(codec)
            .withDictionaryEncoding(true)
            .withDictionaryPageSize(dictionaryPageSize)
            .withPageSize(pageSize)
            .withRowGroupSize(rowGroupSize)
            .withDataModel(AvroConverter.genericData())
            .withSchema(schema);

        // convert
        try (
            org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = parquetWriterBuilder.build();
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))
        )
        {
            Long lineCount = this.convert(inputStream, schema, writer::write);

            // metrics & finalize
            runContext.metric(Counter.of("records", lineCount));

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }

    public enum CompressionCodec {
        UNCOMPRESSED,
        SNAPPY,
        GZIP,
        ZSTD;

        CompressionCodecName parquetCodec() {
            return CompressionCodecName.valueOf(this.name().toUpperCase(Locale.ENGLISH));
        }
    }

    public enum Version {
        V1,
        V2
    }
}
