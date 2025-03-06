package io.kestra.plugin.serdes.parquet;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.serdes.avro.AbstractAvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverter;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.Locale;

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
                namespace: company.team
                
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
    private Property<String> from;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The compression to used"
    )
    Property<CompressionCodec> compressionCodec = Property.of(CompressionCodec.GZIP);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target row group size"
    )
    private Property<Version> parquetVersion = Property.of(Version.V2);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target row group size"
    )
    private Property<Long> rowGroupSize = Property.of((long) org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Target page size"
    )
    private Property<Integer> pageSize = Property.of(ParquetWriter.DEFAULT_PAGE_SIZE);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Max dictionary page size"
    )
    private Property<Integer> dictionaryPageSize = Property.of(ParquetWriter.DEFAULT_PAGE_SIZE);

    static {
        ParquetTools.handleLogger();

        // We initialize snappy in a static initializer block, so it is done when the plugin is loaded by the plugin registry,
        // and not at when it is executed by the Worker to prevent issues with Java Security that prevent writing on /tmp.
        ParquetTools.initSnappy();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file, we create multiple useless tree to avoid incompatibility with EE javaSecurity
        java.nio.file.Path tempDir = runContext.workingDir().path().resolve(IdUtils.create());
        tempDir.toFile().mkdirs();
        File tempFile = Files.createTempFile(tempDir, "", ".parquet").toFile();

        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro options
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // parquet options
        CompressionCodecName codec = runContext.render(this.compressionCodec).as(CompressionCodec.class).orElseThrow().parquetCodec();
        HadoopOutputFile outfileFile = HadoopOutputFile.fromPath(new Path(tempFile.getPath()), new Configuration());

        AvroParquetWriter.Builder<GenericData.Record> parquetWriterBuilder = AvroParquetWriter
            .<GenericData.Record>builder(outfileFile)
            .withWriterVersion(runContext.render(parquetVersion).as(Version.class).orElseThrow() == Version.V2 ? PARQUET_2_0 : PARQUET_1_0)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(codec)
            .withDictionaryEncoding(true)
            .withDictionaryPageSize(runContext.render(dictionaryPageSize).as(Integer.class).orElseThrow())
            .withPageSize(runContext.render(pageSize).as(Integer.class).orElseThrow())
            .withRowGroupSize(runContext.render(rowGroupSize).as(Long.class).orElseThrow())
            .withDataModel(AvroConverter.genericData())
            .withSchema(schema);

        // convert
        try (
            org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = parquetWriterBuilder.build();
            Reader inputStream = new InputStreamReader(runContext.storage().getFile(from))
        ) {
            Long lineCount = this.convert(inputStream, schema, writer::write, runContext);

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
