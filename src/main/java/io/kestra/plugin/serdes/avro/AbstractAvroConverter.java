package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.validations.DateFormat;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

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
    @PluginProperty(dynamic = true)
    protected final List<String> trueValues = Arrays.asList("t", "true", "enabled", "1", "on", "yes");

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as False"
    )
    @PluginProperty(dynamic = true)
    protected final List<String> falseValues = Arrays.asList("f", "false", "disabled", "0", "off", "no", "");

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as null"
    )
    @PluginProperty(dynamic = true)
    protected final List<String> nullValues = Arrays.asList(
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
    );

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
    @PluginProperty(dynamic = true)
    protected final Character decimalSeparator = '.';

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Whether to consider a field present in the data but not declared in the schema as an error",
        description = "Default value is false"
    )
    @PluginProperty
    protected Boolean strictSchema = Boolean.FALSE;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Try to infer all fields",
        description = "If true, we try to infer all fields with `trueValues`, `trueValues` & `nullValues`." +
            "If false, we will infer bool & null only on field declared on schema as `null` and `bool`."
    )
    @PluginProperty
    protected Boolean inferAllFields = false;

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Timezone to use when no timezone can be parsed on the source.",
        description = "If null, the timezone will be `UTC` Default value is system timezone"
    )
    @PluginProperty
    protected final String timeZoneId = ZoneId.systemDefault().toString();


    protected <E extends Exception> Long convert(Reader inputStream, Schema schema, Rethrow.ConsumerChecked<GenericData.Record, E> consumer) throws IOException {
        AvroConverter converter = new AvroConverter(this);

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
