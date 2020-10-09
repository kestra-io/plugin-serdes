package org.kestra.task.serdes.avro;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

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
        description = "Source file URI",
        dynamic = true
    )
    private String from;

    @NotNull
    @InputProperty(
        description = "The avro schema associated to the data",
        dynamic = true
    )
    private String schema;

    @InputProperty(
        description = "Values to consider as True",
        body = "Default values are \"t\", \"true\", \"enabled\", \"1\", \"on\", \"yes\"",
        dynamic = false
    )
    private List<String> trueValues;

    @InputProperty(
        description = "Values to consider as False",
        body = "Default values are \"f\", \"false\", \"disabled\", \"0\", \"off\", \"no\", \"\"",
        dynamic = false
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
            "        \"null\"",
        dynamic = false
    )
    private List<String> nullValues;

    @InputProperty(
        description = "Format to use when parsing date",
        body = "Default value is yyyy-MM-dd[XXX].",
        dynamic = false
    )
    private String dateFormat;

    @InputProperty(
        description = "Format to use when parsing time",
        body = "Default value is HH:mm[:ss][.SSSSSS][XXX]",
        dynamic = false
    )
    private String timeFormat;

    @InputProperty(
        description = "Format to use when parsing datetime",
        body = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]",
        dynamic = false
    )
    private String datetimeFormat;

    @InputProperty(
        description = "Character to recognize as decimal point (e.g. use ‘,’ for European data).",
        body = "Default value is '.'",
        dynamic = false
    )
    private Character decimalSeparator;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".avro");
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));

        // avro writer
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(this.schema));

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema, AvroConverter.genericData());

        // reader
        URI from = new URI(runContext.render(this.from));

        try (
            ObjectInputStream inputStream = new ObjectInputStream(runContext.uriToInputStream(from));
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            DataFileWriter<GenericRecord> schemaDdataFileWriter = dataFileWriter.create(schema, output);
        ) {
            Flowable<GenericData.Record> flowable = Flowable
                .create(ObjectsSerde.reader(inputStream), BackpressureStrategy.BUFFER)
                .map(this.convertToAvro(schema))
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
                });


            // metrics & finalize
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
            try {
                if (row instanceof List) {
                    List<String> casted = (List<String>) row;

                    return converter.fromArray(schema, casted);
                } else if (row instanceof Map) {
                    Map<String, Object> casted = (Map<String, Object>) row;

                    return converter.fromMap(schema, casted);
                }

                return record;
            } catch (Throwable e) {
                throw new AvroConverter.IllegalRow(
                    row,
                    e
                );
            }
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

        if (this.decimalSeparator != null) {
            builder.decimalSeparator(this.decimalSeparator);
        }

        return builder.build();
    }
}
