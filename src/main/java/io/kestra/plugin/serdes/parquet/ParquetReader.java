package io.kestra.plugin.serdes.parquet;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroDeserializer;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Single;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided parquet file and convert it to ion serialized data file."
)
public class ParquetReader extends Task implements RunnableTask<ParquetReader.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    static {
        ParquetTools.handleLogger();
    }

    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // New ion file
        File tempFile = runContext.tempFile(".ion").toFile();

        // Parquet file
        File parquetFile = runContext.tempFile(".parquet").toFile();
        try (OutputStream outputStream = new FileOutputStream(parquetFile)) {
            IOUtils.copyLarge(runContext.uriToInputStream(from), outputStream);
        }
        Path parquetHadoopPath = new Path(parquetFile.getPath());
        HadoopInputFile parquetOutputFile = HadoopInputFile.fromPath(parquetHadoopPath, new Configuration());

        AvroParquetReader.Builder<GenericRecord> parquetReaderBuilder = AvroParquetReader.<GenericRecord>builder(parquetOutputFile)
            .disableCompatibility()
            .withDataModel(AvroConverter.genericData());

        try (
            final org.apache.parquet.hadoop.ParquetReader<GenericRecord> parquetReader = parquetReaderBuilder.build();
            OutputStream output = new FileOutputStream(tempFile)
        ) {

            Flowable<Map<String, Object>> flowable = Flowable
                .create(this.nextRow(parquetReader), BackpressureStrategy.BUFFER)
                .map(AvroDeserializer::recordDeserializer)
                .doOnNext(row -> FileSerde.write(output, row));

            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();
            runContext.metric(Counter.of("records", lineCount));

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    private FlowableOnSubscribe<GenericRecord> nextRow(org.apache.parquet.hadoop.ParquetReader<GenericRecord> parquetReader) {
        return s -> {
            boolean next = true;
            while (next) {
                GenericRecord record = parquetReader.read();

                if (record == null) {
                    next = false;
                } else {
                    s.onNext(record);
                }
            }

            s.onComplete();
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }
}
