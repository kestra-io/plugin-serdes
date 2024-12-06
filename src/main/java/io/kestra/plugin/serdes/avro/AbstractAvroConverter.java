package io.kestra.plugin.serdes.avro;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.validations.DateFormat;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.Reader;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAvroConverter extends Task {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The avro schema associated to the data"
    )
    @AvroSchemaValidation
    @PluginProperty(dynamic = true)
    protected String schema;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as True"
    )
    protected final Property<List<String>> trueValues = Property.of(Arrays.asList("t", "true", "enabled", "1", "on", "yes"));

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as False"
    )
    protected final Property<List<String>> falseValues = Property.of(Arrays.asList("f", "false", "disabled", "0", "off", "no", ""));

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as null"
    )
    protected final Property<List<String>> nullValues = Property.of(Arrays.asList(
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "1.#IND",
        "1.#QNAN",
        "NA",
        "n/a",
        "nan",
        "null"
    ));

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing date"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String dateFormat = "yyyy-MM-dd[XXX]";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing time"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String timeFormat = "HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Format to use when parsing datetime",
        description = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String datetimeFormat = "yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Character to recognize as decimal point (e.g. use ‘,’ for European data).",
        description = "Default value is '.'"
    )
    protected final Property<Character> decimalSeparator = Property.of('.');

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Whether to consider a field present in the data but not declared in the schema as an error",
        description = "Default value is false"
    )
    protected Property<Boolean> strictSchema = Property.of(Boolean.FALSE);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Try to infer all fields",
        description = "If true, we try to infer all fields with `trueValues`, `trueValues` & `nullValues`." +
            "If false, we will infer bool & null only on field declared on schema as `null` and `bool`."
    )
    protected Property<Boolean> inferAllFields = Property.of(false);

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source.",
        description = "If null, the timezone will be `UTC` Default value is system timezone"
    )
    protected final Property<String> timeZoneId = Property.of(ZoneId.systemDefault().toString());


    protected <E extends Exception> Long convert(Reader inputStream, Schema schema, Rethrow.ConsumerChecked<GenericData.Record, E> consumer, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        AvroConverter converter = AvroConverter.builder()
            .schema(runContext.render(this.schema))
            .nullValues(runContext.render(this.nullValues).asList(String.class))
            .trueValues(runContext.render(this.trueValues).asList(String.class))
            .falseValues(runContext.render(this.falseValues).asList(String.class))
            .dateFormat(runContext.render(this.dateFormat))
            .timeFormat(runContext.render(this.timeFormat))
            .datetimeFormat(runContext.render(this.datetimeFormat))
            .decimalSeparator(runContext.render(this.decimalSeparator).as(Character.class).orElseThrow())
            .strictSchema(runContext.render(this.strictSchema).as(Boolean.class).orElseThrow())
            .inferAllFields(runContext.render(this.inferAllFields).as(Boolean.class).orElseThrow())
            .timeZoneId(runContext.render(this.timeZoneId).as(String.class).orElseThrow())
            .build();

        Flux<GenericData.Record> flowable = FileSerde.readAll(inputStream)
            .map(this.convertToAvro(schema, converter))
            .doOnNext(datum -> {
                try {
                    consumer.accept(datum);
                } catch (Throwable e) {
                    var avroException =  new AvroConverter.IllegalRowConvertion(
                        datum.getSchema()
                            .getFields()
                            .stream()
                            .map(field -> new AbstractMap.SimpleEntry<>(field.name(), datum.get(field.name())))
                            // https://bugs.openjdk.java.net/browse/JDK-8148463
                            .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll),
                        e,
                        null
                    );
                    throw new RuntimeException(avroException);
                }
            });

        // metrics & finalize
        Mono<Long> count = flowable.count();
        return count.block();
    }

    @SuppressWarnings("unchecked")
    protected Function<Object, GenericData.Record> convertToAvro(Schema schema, AvroConverter converter) {
        return row -> {
            try {
                if (row instanceof List) {
                    List<String> casted = (List<String>) row;

                    return converter.fromArray(schema, casted);
                } else if (row instanceof Map) {
                    Map<String, Object> casted = (Map<String, Object>) row;

                    return converter.fromMap(schema, casted);
                }

                throw new IllegalArgumentException("Unable to convert row of type: " + row.getClass());
            } catch (Throwable e) {
                var avroException =  new AvroConverter.IllegalRow(
                    row,
                    e
                );
                throw new RuntimeException(avroException);
            }
        };
    }
}
