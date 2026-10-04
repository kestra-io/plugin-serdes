package io.kestra.plugin.serdes.parquet;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.serdes.avro.AbstractAvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.infer.InferAvroSchema;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an Ion file to the Parquet format",
    description = """
        An Avro schema is required to define column types; if none is provided, \
        one is inferred by scanning up to `numberOfRowsToScan` rows. Supports \
        configurable compression (default: `GZIP`), Parquet format version, \
        row group size, page size, and dictionary page size.

        Under `onBadLines: WARN` or `SKIP`, only rows that fail conversion to the Avro schema are skipped. \
        A value that converts successfully but is later rejected by the Parquet encoder — for example a \
        `decimal` whose unscaled value overflows its declared `fixed` size — aborts the underlying writer, \
        so the task fails on the row following the one that actually caused the problem, with a \
        "Writer has been aborted" error."""
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
        ),
        @Example(
            full = true,
            title = "Convert a CSV file to Parquet, logging a warning and skipping any row that fails to match the schema instead of failing the task.",
            code = """
                id: ion_to_parquet_on_bad_lines
                namespace: company.team

                tasks:
                  - id: write_csv
                    type: io.kestra.plugin.core.storage.Write
                    extension: .csv
                    content: |
                      work_year,job_title,salary_in_usd
                      2023,Data Scientist,120000
                      2023,Data Engineer,
                      2022,Data Analyst,95000

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.write_csv.uri }}"

                  - id: result
                    type: io.kestra.plugin.serdes.parquet.IonToParquet
                    from: "{{ outputs.convert.uri }}"
                    onBadLines: WARN
                    schema: |
                      {
                        "type": "record",
                        "name": "Salary",
                        "namespace": "com.example.salary",
                        "fields": [
                          {"name": "work_year", "type": "int"},
                          {"name": "job_title", "type": "string"},
                          {"name": "salary_in_usd", "type": "int"}
                        ]
                      }
                """
        ),
        @Example(
            full = true,
            title = "Convert a CSV file to a ZSTD-compressed Parquet file with a high compression level, for long-term storage.",
            code = """
                id: ion_to_parquet_zstd_level
                namespace: company.team

                tasks:
                  - id: download_csv
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.download_csv.uri }}"

                  - id: result
                    type: io.kestra.plugin.serdes.parquet.IonToParquet
                    from: "{{ outputs.convert.uri }}"
                    compressionCodec: ZSTD
                    compressionLevel: 19
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records written", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.parquet.ParquetWriter"
)
public class IonToParquet extends AbstractAvroConverter implements RunnableTask<IonToParquet.Output> {
    // read by org.apache.parquet.hadoop.codec.ZstandardCodec, and part of CodecFactory's codec cache key
    private static final String ZSTD_LEVEL_CONF_KEY = "parquet.compression.codec.zstd.level";
    // bounds of the Zstandard library: ZSTD_minCLevel() (negative "fast" levels) and ZSTD_maxCLevel()
    private static final int ZSTD_MIN_LEVEL = -(1 << 17);
    private static final int ZSTD_MAX_LEVEL = 22;

    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "Compression codec"
    )
    Property<CompressionCodec> compressionCodec = Property.ofValue(CompressionCodec.GZIP);

    @Schema(
        title = "Compression level",
        description = """
            Only supported with the `ZSTD` codec; setting it with any other codec fails the task. \
            Accepts values from `-131072` to `22`. Levels `1` to `22` trade write speed for smaller files: \
            higher is smaller but slower, and levels `20` to `22` also use much more memory, \
            so `19` is a practical choice for long-term storage. \
            Negative levels compress faster than `1` but produce larger files. \
            `0`, like leaving this property unset, uses the Zstandard default level (`3`)."""
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> compressionLevel;

    @Builder.Default
    @Schema(
        title = "Parquet format version"
    )
    @PluginProperty(group = "advanced")
    private Property<Version> parquetVersion = Property.ofValue(Version.V2);

    @Builder.Default
    @Schema(
        title = "Target row group size"
    )
    @PluginProperty(group = "advanced")
    private Property<Long> rowGroupSize = Property.ofValue((long) org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE);

    @Builder.Default
    @Schema(
        title = "Target page size"
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> pageSize = Property.ofValue(ParquetWriter.DEFAULT_PAGE_SIZE);

    @Builder.Default
    @Schema(
        title = "Max dictionary page size"
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> dictionaryPageSize = Property.ofValue(ParquetWriter.DEFAULT_PAGE_SIZE);

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

        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // avro schema
        var schemaParser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema schema;
        if (this.schema == null) {
            try (var inputStreamForInfer = runContext.storage().getFile(from)) {
                var schemaOutputStream = new ByteArrayOutputStream();
                new InferAvroSchema(
                    getEffectiveRowsToScan(runContext)
                ).inferAvroSchemaFromIon(inputStreamForInfer, schemaOutputStream);
                schema = schemaParser.parse(schemaOutputStream.toString(StandardCharsets.UTF_8));
            }
        } else {
            schema = schemaParser.parse(runContext.render(this.schema));
        }

        // parquet options
        CompressionCodec rCompressionCodec = runContext.render(this.compressionCodec).as(CompressionCodec.class).orElseThrow();
        CompressionCodecName codec = rCompressionCodec.parquetCodec();

        Configuration conf = new Configuration();
        Integer rCompressionLevel = runContext.render(this.compressionLevel).as(Integer.class).orElse(null);
        if (rCompressionLevel != null) {
            if (rCompressionCodec != CompressionCodec.ZSTD) {
                throw new IllegalArgumentException(
                    "compressionLevel is only supported with the ZSTD compression codec, but compressionCodec is " + rCompressionCodec +
                        ". Set compressionCodec to ZSTD or remove compressionLevel."
                );
            }
            if (rCompressionLevel < ZSTD_MIN_LEVEL || rCompressionLevel > ZSTD_MAX_LEVEL) {
                throw new IllegalArgumentException(
                    "Invalid ZSTD compressionLevel " + rCompressionLevel + ". Use a value between " + ZSTD_MIN_LEVEL + " and " + ZSTD_MAX_LEVEL +
                        " (higher values compress more), or remove compressionLevel to use the default level 3."
                );
            }
            conf.setInt(ZSTD_LEVEL_CONF_KEY, rCompressionLevel);
        }

        HadoopOutputFile outfileFile = HadoopOutputFile.fromPath(new Path(tempFile.getPath()), conf);

        AvroParquetWriter.Builder<GenericData.Record> parquetWriterBuilder = AvroParquetWriter.<GenericData.Record> builder(outfileFile)
            .withWriterVersion(runContext.render(parquetVersion).as(Version.class).orElseThrow() == Version.V2 ? PARQUET_2_0 : PARQUET_1_0)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withConf(conf)
            .withCompressionCodec(codec)
            .withDictionaryEncoding(true)
            .withDictionaryPageSize(runContext.render(dictionaryPageSize).as(Integer.class).orElseThrow())
            .withPageSize(runContext.render(pageSize).as(Integer.class).orElseThrow())
            .withRowGroupSize(runContext.render(rowGroupSize).as(Long.class).orElseThrow())
            .withDataModel(AvroConverter.genericData())
            .withSchema(schema);

        // convert
        Long lineCount = null;
        try (
            org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = parquetWriterBuilder.build();
            InputStream inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)
        ) {
            lineCount = this.convert(inputStream, schema, writer::write, runContext);

            // metrics & finalize
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(lineCount != null ? lineCount : 0L)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;

        @Schema(title = "The number of records written", description = "Rows skipped under `onBadLines: WARN` or `SKIP` are not counted.")
        private long size;
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
