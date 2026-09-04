package io.kestra.plugin.serdes.avro;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericData;

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
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

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
    @PluginProperty(dynamic = true, group = "advanced")
    protected String schema;

    @Builder.Default
    @Schema(
        title = "Number of rows that will be scanned while inferring. The more rows scanned, the more precise the output schema will be",
        description = """
            Only use when the 'schema' property is empty. \
            Ignored for schema inference when `inferAllFields` is `true` — in that case, all rows are scanned."""
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> numberOfRowsToScan = Property.ofValue(100);

    @Builder.Default
    @Schema(
        title = "Values to consider as True"
    )
    @PluginProperty(group = "advanced")
    protected final Property<List<String>> trueValues = Property.ofValue(Arrays.asList("t", "true", "enabled", "1", "on", "yes"));

    @Builder.Default
    @Schema(
        title = "Values to consider as False"
    )
    @PluginProperty(group = "advanced")
    protected final Property<List<String>> falseValues = Property.ofValue(Arrays.asList("f", "false", "disabled", "0", "off", "no", ""));

    @Builder.Default
    @Schema(
        title = "Values to consider as null"
    )
    @PluginProperty(group = "advanced")
    protected final Property<List<String>> nullValues = Property.ofValue(
        Arrays.asList(
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
        )
    );

    @Builder.Default
    @Schema(
        title = "Format to use when parsing date"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    protected final String dateFormat = "yyyy-MM-dd[XXX]";

    @Builder.Default
    @Schema(
        title = "Format to use when parsing time"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    protected final String timeFormat = "HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @Schema(
        title = "Format to use when parsing datetime",
        description = "Default value is yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"
    )
    @PluginProperty(dynamic = true, group = "processing")
    @DateFormat
    protected final String datetimeFormat = "yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @Schema(
        title = "Character to recognize as decimal point (e.g. use ‘,’ for European data)",
        description = "Default value is '.'"
    )
    @PluginProperty(group = "processing")
    protected final Property<Character> decimalSeparator = Property.ofValue('.');

    @Builder.Default
    @Schema(
        title = "Whether to consider a field present in the data but not declared in the schema as an error",
        description = "Default value is false"
    )
    @PluginProperty(group = "connection")
    protected Property<Boolean> strictSchema = Property.ofValue(Boolean.FALSE);

    @Builder.Default
    @Schema(
        title = "Try to infer all fields",
        description = """
            If `true`, schema inference scans **all rows** (ignoring `numberOfRowsToScan`) and attempts to infer \
            all field types using `trueValues`, `falseValues`, and `nullValues`. \
            This prevents fields that are null in the first scanned rows from being typed as NULL. \
            If `false`, only the first `numberOfRowsToScan` rows are scanned, and booleans/nulls are inferred \
            only on fields declared in the schema as `null` or `bool`."""
    )
    @PluginProperty(group = "advanced")
    protected Property<Boolean> inferAllFields = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Timezone to use when no timezone can be parsed on the source",
        description = "If null, the timezone defaults to `UTC`. Default value is the system timezone"
    )
    @PluginProperty(group = "advanced")
    protected final Property<String> timeZoneId = Property.ofValue(ZoneId.systemDefault().toString());

    @Builder.Default
    @Schema(
        title = "How to handle bad records (e.g., null values in non-nullable fields or type mismatches)",
        description = "Can be `ERROR`, `WARN`, or `SKIP`."
    )
    @PluginProperty(group = "advanced")
    protected final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);

    protected int getEffectiveRowsToScan(RunContext runContext) throws IllegalVariableEvaluationException {
        var rInferAllFields = runContext.render(this.inferAllFields).as(Boolean.class).orElse(false);
        if (Boolean.TRUE.equals(rInferAllFields)) {
            return Integer.MAX_VALUE;
        }
        return runContext.render(this.numberOfRowsToScan).as(Integer.class).orElseThrow();
    }

    protected <E extends Exception> Long convert(InputStream inputStream, org.apache.avro.Schema schema, Rethrow.ConsumerChecked<GenericData.Record, E> consumer, RunContext runContext)
        throws IOException, IllegalVariableEvaluationException {
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

        var writtenCount = new AtomicLong();

        Flux<GenericData.Record> flowable = FileSerde.readAll(inputStream)
            .map(this.convertToAvro(schema, converter, rOnBadLines))
            .doOnNext(datum -> this.writeRecord(datum, consumer, rOnBadLines, runContext, writtenCount));

        // rows skipped under WARN/SKIP must not be counted
        flowable.then().block();
        return writtenCount.get();
    }

    /**
     * Under {@code WARN}/{@code SKIP}, validates the record before handing it to the consumer: a writer such as
     * {@code AvroParquetWriter} is not resumable after a failed {@code write()}, so a bad record must never reach
     * it. {@code ERROR} skips validation, preserving its exact exception type and message.
     */
    private <E extends Exception> void writeRecord(
        GenericData.Record datum,
        Rethrow.ConsumerChecked<GenericData.Record, E> consumer,
        OnBadLines rOnBadLines,
        RunContext runContext,
        AtomicLong writtenCount
    ) {
        if (rOnBadLines != OnBadLines.ERROR) {
            var invalidField = firstNonNullableFieldHoldingNull(datum);
            if (invalidField != null) {
                if (rOnBadLines == OnBadLines.WARN) {
                    runContext.logger().warn("Bad record skipped (onBadLines=WARN): field '{}' of schema '{}' is null but not nullable: {}", invalidField, datum.getSchema().getName(), describeRecord(datum));
                }
                return;
            }
        }

        try {
            consumer.accept(datum);
            writtenCount.incrementAndGet();
        } catch (Throwable e) {
            // ERROR wraps every write failure exactly as it did before this fix, IOException included
            if (rOnBadLines == OnBadLines.ERROR) {
                throw new RuntimeException(illegalRowConvertion(datum, e));
            }

            // an I/O failure is infrastructure, not a bad line: WARN/SKIP must not swallow it
            if (isIOFailure(e)) {
                throw new RuntimeException(e);
            }

            if (rOnBadLines == OnBadLines.WARN) {
                runContext.logger().warn("Bad record skipped (onBadLines=WARN): {}", truncateForLog(e.getMessage()));
            }
            // SKIP: silently drop the row
        }
    }

    /**
     * Name of the first field holding {@code null} under a schema that doesn't admit it, or {@code null} if the
     * record is well-formed -- the single way {@link AvroConverter#fromMap} reports a failed cell under WARN/SKIP.
     * Checks declared nullability only, never the Java type a logical-type conversion produced. Walks the data
     * rather than the schema, so recursive named types cannot loop.
     */
    private static String firstNonNullableFieldHoldingNull(GenericData.Record datum) {
        return firstNonNullableFieldHoldingNull(datum, null);
    }

    private static String firstNonNullableFieldHoldingNull(GenericData.Record datum, String parentFieldName) {
        for (var field : datum.getSchema().getFields()) {
            var currentFieldName = parentFieldName != null ? parentFieldName + "." + field.name() : field.name();
            var invalidField = checkChildValue(datum.get(field.name()), field.schema(), currentFieldName);
            if (invalidField != null) {
                return invalidField;
            }
        }
        return null;
    }

    /** Checks one field/element/map-value slot; fails open when {@code schema} could not be resolved. */
    private static String checkChildValue(Object value, org.apache.avro.Schema schema, String fieldName) {
        if (value == null) {
            return (schema == null || acceptsNull(schema)) ? null : fieldName;
        }
        return firstNonNullableValueHoldingNull(value, schema, fieldName);
    }

    /**
     * Looks for a nested invalid null inside a non-null value: a nested record's fields, or each element of a
     * {@code List}/{@code Map} built by {@code complexArray}/{@code complexMap}. {@code schema} may be a UNION.
     */
    private static String firstNonNullableValueHoldingNull(Object value, org.apache.avro.Schema schema, String fieldName) {
        if (value instanceof GenericData.Record nested) {
            return firstNonNullableFieldHoldingNull(nested, fieldName);
        }

        if (value instanceof Collection<?> collection) {
            var elementSchema = resolveSchema(schema, org.apache.avro.Schema.Type.ARRAY);
            var elementType = elementSchema != null ? elementSchema.getElementType() : null;
            int index = 0;
            for (Object element : collection) {
                var invalidField = checkChildValue(element, elementType, fieldName + "[" + index + "]");
                if (invalidField != null) {
                    return invalidField;
                }
                index++;
            }
            return null;
        }

        if (value instanceof Map<?, ?> map) {
            var mapSchema = resolveSchema(schema, org.apache.avro.Schema.Type.MAP);
            var valueType = mapSchema != null ? mapSchema.getValueType() : null;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                var invalidField = checkChildValue(entry.getValue(), valueType, fieldName + "{'" + entry.getKey() + "'}");
                if (invalidField != null) {
                    return invalidField;
                }
            }
            return null;
        }

        return null;
    }

    /** Returns {@code schema} if it already is of {@code type}, or its matching branch if it's a UNION, else {@code null}. */
    private static org.apache.avro.Schema resolveSchema(org.apache.avro.Schema schema, org.apache.avro.Schema.Type type) {
        if (schema == null) {
            return null;
        }
        if (schema.getType() == type) {
            return schema;
        }
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                .filter(candidate -> candidate.getType() == type)
                .findFirst()
                .orElse(null);
        }
        return null;
    }

    private static boolean acceptsNull(org.apache.avro.Schema schema) {
        return switch (schema.getType()) {
            case NULL -> true;
            case UNION -> schema.getTypes().stream().anyMatch(type -> type.getType() == org.apache.avro.Schema.Type.NULL);
            default -> false;
        };
    }

    /** Caps record dumps in WARN lines, to bound log volume across many bad rows. */
    private static final int MAX_LOGGED_RECORD_LENGTH = 1000;

    private static String truncateForLog(String value) {
        if (value == null || value.length() <= MAX_LOGGED_RECORD_LENGTH) {
            return value;
        }
        // cutting inside a surrogate pair would leave a dangling half
        int cut = Character.isHighSurrogate(value.charAt(MAX_LOGGED_RECORD_LENGTH - 1))
            ? MAX_LOGGED_RECORD_LENGTH - 1
            : MAX_LOGGED_RECORD_LENGTH;
        return value.substring(0, cut) + "… (truncated)";
    }

    // builds the dump field by field so a single oversized field can't force materializing the whole record first
    private static String describeRecord(GenericData.Record datum) {
        var builder = new StringBuilder("{");
        var fields = datum.getSchema().getFields();
        var truncated = false;
        for (int i = 0; i < fields.size(); i++) {
            if (builder.length() >= MAX_LOGGED_RECORD_LENGTH) {
                truncated = true;
                break;
            }
            if (i > 0) {
                builder.append(", ");
            }
            var field = fields.get(i);
            builder.append('"').append(field.name()).append("\": ").append(truncateForLog(String.valueOf(datum.get(field.name()))));
        }
        builder.append(truncated ? ", …}" : "}");
        return builder.toString();
    }

    private static boolean isIOFailure(Throwable e) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (current instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    private static AvroConverter.IllegalRowConvertion illegalRowConvertion(GenericData.Record datum, Throwable e) {
        return new AvroConverter.IllegalRowConvertion(
            datum.getSchema()
                .getFields()
                .stream()
                .map(field -> new AbstractMap.SimpleEntry<>(field.name(), datum.get(field.name())))
                // https://bugs.openjdk.java.net/browse/JDK-8148463
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll),
            e,
            null
        );
    }

    @SuppressWarnings("unchecked")
    protected Function<Object, GenericData.Record> convertToAvro(org.apache.avro.Schema schema, AvroConverter converter, OnBadLines onBadLines) {
        return row ->
        {
            try {
                if (row instanceof List) {
                    List<?> casted = (List<?>) row;
                    var fields = schema.getFields();
                    // Root Ion array: schema was wrapped in a record with a single array-typed field.
                    // The entire list is the value of that field, not a positional record.
                    if (fields.size() == 1 && fields.getFirst().schema().getType() == org.apache.avro.Schema.Type.ARRAY) {
                        return converter.fromMap(schema, Map.of(fields.getFirst().name(), casted), onBadLines, null);
                    }
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
