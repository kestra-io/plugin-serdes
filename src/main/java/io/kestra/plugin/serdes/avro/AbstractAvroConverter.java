package io.kestra.plugin.serdes.avro;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.validations.DateFormat;
import io.kestra.plugin.serdes.OnBadLines;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.generic.GenericData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.Reader;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAvroConverter extends Task {
    @Schema(
        title = "The avro schema associated with the data",
        description = "If empty, the task will try to infer the schema from the current data; use the 'numberOfRowsToScan' property if needed"
    )
    @AvroSchemaValidation
    @PluginProperty(dynamic = true)
    protected String schema;

    @Builder.Default
    @Schema(
        title = "Number of rows that will be scanned while inferring. The more rows scanned, the more precise the output schema will be.",
        description = "Only use when the 'schema' property is empty"
    )
    private Property<Integer> numberOfRowsToScan = Property.ofValue(100);

    @Builder.Default
    @Schema(
        title = "Values to consider as True"
    )
    protected final Property<List<String>> trueValues = Property.ofValue(Arrays.asList("t", "true", "enabled", "1", "on", "yes"));

    @Builder.Default
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Values to consider as False"
    )
    protected final Property<List<String>> falseValues = Property.ofValue(Arrays.asList("f", "false", "disabled", "0", "off", "no", ""));

    @Builder.Default
    @Schema(
        title = "Values to consider as null"
    )
    protected final Property<List<String>> nullValues = Property.ofValue(Arrays.asList(
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
    @Schema(
        title = "Format to use when parsing date"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String dateFormat = "yyyy-MM-dd[XXX]";

    @Builder.Default
    @Schema(
        title = "Format to use when parsing time"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String timeFormat = "HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @Schema(
        title = "Format to use when parsing datetime",
        description = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"
    )
    @PluginProperty(dynamic = true)
    @DateFormat
    protected final String datetimeFormat = "yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @Schema(
        title = "Character to recognize as decimal point (e.g. use ‘,’ for European data).",
        description = "Default value is '.'"
    )
    protected final Property<Character> decimalSeparator = Property.ofValue('.');

    @Builder.Default
    @Schema(
        title = "Whether to consider a field present in the data but not declared in the schema as an error",
        description = "Default value is false"
    )
    protected Property<Boolean> strictSchema = Property.ofValue(Boolean.FALSE);

    @Builder.Default
    @Schema(
        title = "Try to infer all fields",
        description = "If true, we try to infer all fields using `trueValues`, `falseValues`, and `nullValues`." +
            "If false, we infer booleans and nulls only on fields declared in the schema as `null` or `bool`."
    )
    protected Property<Boolean> inferAllFields = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Timezone to use when no timezone can be parsed on the source.",
        description = "If null, the timezone defaults to `UTC`. Default value is the system timezone"
    )
    protected final Property<String> timeZoneId = Property.ofValue(ZoneId.systemDefault().toString());

    @Builder.Default
    @Schema(
        title = "How to handle bad records (e.g., null values in non-nullable fields or type mismatches).",
        description = "Can be one of: `FAIL`, `WARN` or `SKIP`."
    )
    protected final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);


    protected <E extends Exception> Long convert(Reader inputStream, org.apache.avro.Schema schema, Rethrow.ConsumerChecked<GenericData.Record, E> consumer, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        OnBadLines rOnBadLines = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);
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
            .timeZoneId(runContext.render(this.timeZoneId).as(String.class).orElse(ZoneId.systemDefault().toString()))
            .onBadLines(rOnBadLines)
            .build();

        Flux<GenericData.Record> flowable = FileSerde.readAll(inputStream)
            .map(this.convertToAvro(schema, converter, rOnBadLines))
            .doOnNext(datum -> {
                try {
                    consumer.accept(datum);
                } catch (Throwable e) {
                    var avroException = new AvroConverter.IllegalRowConvertion(
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
    protected Function<Object, GenericData.Record> convertToAvro( org.apache.avro.Schema schema, AvroConverter converter, OnBadLines onBadLines) {
        return row -> {
            try {
                if (row instanceof List) {
                    List<?> casted = (List<?>) row;  // Allow Object for flexibility
                    return converter.fromArray(schema, casted, onBadLines);
                } else if (row instanceof Map) {
                    Map<String, Object> mapRow = (Map<String, Object>) row;
                    // Detect positional colN keys (e.g., from CsvToIon with header=false)
                    Set<String> keys = mapRow.keySet();
                    boolean isPositional = keys.stream()
                        .allMatch(k -> k.startsWith("col") && k.length() > 3 && k.substring(3).matches("\\d+"));
                    if (isPositional) {
                        int dataFieldCount = keys.size();
                        int schemaFieldCount = schema.getFields().size();
                        boolean strict = converter.getStrictSchema();
                        if (strict && dataFieldCount > schemaFieldCount) {
                            // Strict violation: Extra fields detected
                            List<String> fieldNames = new ArrayList<>(keys);
                            List<Object> values = new ArrayList<>(mapRow.values());
                            throw new AvroConverter.IllegalStrictRowConversion(schema, fieldNames, values);
                        }
                        List<Integer> indices = keys.stream()
                            .map(k -> Integer.parseInt(k.substring(3)))
                            .sorted()
                            .collect(Collectors.toList());
                        List<Object> positional = new ArrayList<>();
                        for (int i = 0; i < schemaFieldCount; i++) {
                            int finalI = i;
                            Optional<Object> valueOpt = indices.stream()
                                .filter(idx -> idx == finalI)
                                .map(idx -> mapRow.get("col" + idx))
                                .findFirst();
                            positional.add(valueOpt.orElse(null));
                        }
                        return converter.fromArray(schema, positional, onBadLines);
                    } else {
                        Map<String, Object> casted = mapRow;
                        return converter.fromMap(schema, casted, onBadLines, null);
                    }
                }
                throw new IllegalArgumentException("Unable to convert row of type: " + row.getClass());
            } catch (Throwable e) {
                var avroException = new AvroConverter.IllegalRow(row, e);
                throw new RuntimeException(avroException);
            }
        };
    }
}
