package org.kestra.task.serdes.avro;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.serdes.serializers.ObjectsSerde;

import javax.validation.constraints.NotNull;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Read a provided file containing java serialized data and convert it to avro."
)
public class AvroWriter extends Task implements RunnableTask<AvroWriter.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI"
    )
    private String from;

    @NotNull
    @InputProperty(
        description = "The avro schema associated to the data"
    )
    private String schema;

    @InputProperty(
        description = "Values to consider as True",
        body = "Default values are \"t\", \"true\", \"enabled\", \"1\", \"on\", \"yes\""
    )
    private List<String> trueValues;

    @InputProperty(
        description = "Values to consider as False",
        body = "Default values are \"f\", \"false\", \"disabled\", \"0\", \"off\", \"no\", \"\""
    )
    private List<String> falseValues;

    @InputProperty(
        description = "Values to consider as null",
        body = "Default values are \"\",\n" +
            "        \"#N/A\",\n" +
            "        \"#N/A N/A\",\n" +
            "        \"#NA\",\n" +
            "        \"-1.#IND\",\n" +
            "        \"-1.#QNAN\",\n" +
            "        \"-NaN\",\n" +
            "        \"1.#IND\",\n" +
            "        \"1.#QNAN\",\n" +
            "        \"NA\",\n" +
            "        \"n/a\",\n" +
            "        \"nan\",\n" +
            "        \"null\""
    )
    private List<String> nullValues;

    @InputProperty(
        description = "Format to use when parsing date",
        body = "Default value is yyyy-MM-dd[XXX]."
    )
    private String dateFormat;

    @InputProperty(
        description = "Format to use when parsing time",
        body = "Default value is HH:mm[:ss][.SSSSSS][XXX]"
    )
    private String timeFormat;

    @InputProperty(
        description = "Format to use when parsing datetime",
        body = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"
    )
    private String datetimeFormat;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".avro");
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro writer
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema, AvroConverter.genericData());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, output);

        // reader
        URI from = new URI(runContext.render(this.from));
        ObjectInputStream inputStream = new ObjectInputStream(runContext.uriToInputStream(from));

        // convert
        Flowable<GenericData.Record> flowable = Flowable
            .create(ObjectsSerde.reader(inputStream), BackpressureStrategy.BUFFER)
            .observeOn(Schedulers.computation())
            .map(this.convertToAvro(schema))
            .observeOn(Schedulers.io())
            .doOnNext(datum -> {
                try {
                    dataFileWriter.append(datum);
                } catch (Throwable e) {
                    throw new AvroConverter.IllegalRowConvertion(
                        datum.getSchema()
                            .getFields()
                            .stream()
                            .map(field -> new AbstractMap.SimpleEntry<>(field.name(), datum.get(field.name())))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                        e,
                        null
                    );
                }
            })
            .doOnComplete(() -> {
                dataFileWriter.close();
                inputStream.close();
                output.close();
            });

        // metrics & finalize
        Single<Long> count = flowable.count();
        Long lineCount = count.blockingGet();
        runContext.metric(Counter.of("records", lineCount));

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "URI of a temporary result file"
        )
        private URI uri;
    }

    @SuppressWarnings("unchecked")
    private Function<Object, GenericData.Record> convertToAvro(Schema schema) {
        AvroConverter converter = this.converter();
        GenericData.Record record = new GenericData.Record(schema);

        return row -> {
            if (row instanceof List) {
                List<String> casted = (List<String>) row;

                return converter.fromArray(schema, casted);
            } else if (row instanceof Map) {
                Map<String, Object> casted = (Map<String, Object>) row;

                return converter.fromMap(schema, casted);
            }

            return record;
        };
    }

    private AvroConverter converter() {
        AvroConverter.AvroConverterBuilder builder = AvroConverter.builder();

        if (this.trueValues != null) {
            builder.trueValues(this.trueValues);
        }

        if (this.falseValues != null) {
            builder.falseValues(this.falseValues);
        }

        if (this.nullValues != null) {
            builder.nullValues(this.nullValues);
        }

        if (this.dateFormat != null) {
            builder.dateFormat(this.dateFormat);
        }

        if (this.timeFormat != null) {
            builder.timeFormat(this.timeFormat);
        }

        if (this.datetimeFormat != null) {
            builder.datetimeFormat(this.datetimeFormat);
        }

        return builder.build();
    }
}
