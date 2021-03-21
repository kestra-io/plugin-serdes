package io.kestra.plugin.serdes.avro;

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
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import javax.validation.constraints.NotNull;
import java.io.*;
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
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Read a provided file containing ion serialized data and convert it to avro."
)
public class AvroWriter extends Task implements RunnableTask<AvroWriter.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The avro schema associated to the data"
    )
    @AvroSchemaValidation
    @PluginProperty(dynamic = true)
    private String schema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as True",
        description = "Default values are \"t\", \"true\", \"enabled\", \"1\", \"on\", \"yes\""
    )
    @PluginProperty(dynamic = true)
    private List<String> trueValues;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as False",
        description = "Default values are \"f\", \"false\", \"disabled\", \"0\", \"off\", \"no\", \"\""
    )
    @PluginProperty(dynamic = true)
    private List<String> falseValues;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as null",
        description = "Default values are \"\",\n" +
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
    @PluginProperty(dynamic = true)
    private List<String> nullValues;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing date",
        description = "Default value is yyyy-MM-dd[XXX]."
    )
    @PluginProperty(dynamic = true)
    @DateFormatValidation
    private String dateFormat;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing time",
        description = "Default value is HH:mm[:ss][.SSSSSS][XXX]"
    )
    @PluginProperty(dynamic = true)
    @DateFormatValidation
    private String timeFormat;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing datetime",
        description = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"
    )
    @PluginProperty(dynamic = true)
    @DateFormatValidation
    private String datetimeFormat;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Character to recognize as decimal point (e.g. use ‘,’ for European data).",
        description = "Default value is '.'"
    )
    @PluginProperty(dynamic = true)
    private Character decimalSeparator;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Whether to consider a field present in the data but not declared in the schema as an error",
        description = "Default value is false"
    )
    @PluginProperty(dynamic = false)
    protected Boolean strictSchema = Boolean.FALSE;

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
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            DataFileWriter<GenericRecord> schemaDdataFileWriter = dataFileWriter.create(schema, output);
        ) {
            Flowable<GenericData.Record> flowable = Flowable
                .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
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

        if (this.strictSchema != null) {
            builder.strictSchema(this.strictSchema);
        }

        return builder.build();
    }
}
