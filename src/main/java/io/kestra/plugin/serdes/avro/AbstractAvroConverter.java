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

        AtomicLong writtenCount = new AtomicLong();

        Flux<GenericData.Record> flowable = FileSerde.readAll(inputStream)
            .map(this.convertToAvro(schema, converter, rOnBadLines))
            .doOnNext(datum -> this.writeRecord(datum, consumer, rOnBadLines, runContext, writtenCount));

        // metrics & finalize: count only rows actually written, not rows skipped under WARN/SKIP
        flowable.then().block();
        return writtenCount.get();
    }

    /**
     * A record that fails to write is not safely resumable: {@code AvroParquetWriter} streams field values into
     * column writers as it walks the record, so a partially-written record can leave a row group with columns of
     * unequal length. Under {@code WARN}/{@code SKIP} we therefore validate the record *before* handing it to the
     * consumer, so a bad record never reaches {@code writer.write()} in the first place. Under {@code ERROR},
     * validation is skipped so the existing exception type, message and behaviour are preserved exactly.
     * <p>
     * The only way {@link AvroConverter#fromMap} produces an invalid record under {@code WARN}/{@code SKIP} is by
     * putting {@code null} into a field whose conversion failed (see {@code AvroConverter.fromMap}'s catch blocks):
     * so gating on "does any non-nullable field hold null" catches exactly that failure mode. We deliberately do
     * not use {@code GenericData.validate()} here: it switches on each field's physical Avro type and ignores
     * registered logical-type conversions, so it rejects the {@code BigDecimal}/{@code UUID}/{@code LocalDate}/
     * {@code Instant}/... values {@link AvroConverter#convert} legitimately produces for logical-typed fields,
     * dropping every row of any schema that uses a logical type.
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
                    runContext.logger().warn("Bad record skipped (onBadLines=WARN): field '{}' of schema '{}' is null but not nullable: {}", invalidField, datum.getSchema().getName(), datum);
                }
                return;
            }
        }

        try {
            consumer.accept(datum);
            writtenCount.incrementAndGet();
        } catch (Throwable e) {
            // an I/O failure (disk full, closed stream) is an infrastructure failure, not a bad line: it must fail the task under every mode
            if (isIOFailure(e)) {
                throw new RuntimeException(e);
            }

            if (rOnBadLines == OnBadLines.ERROR) {
                throw new RuntimeException(illegalRowConvertion(datum, e));
            } else if (rOnBadLines == OnBadLines.WARN) {
                runContext.logger().warn("Bad record skipped (onBadLines=WARN): {}", e.getMessage());
            }
            // SKIP: silently drop the row
        }
    }

    /**
     * Returns the name of the first field that is {@code null} while its schema does not admit {@code NULL}, or
     * {@code null} if the record is well-formed. Conversion-agnostic on purpose: it inspects the physical
     * nullability of the field's declared schema, never the Java type a logical-type conversion produced.
     * Recurses into nested records: {@code AvroConverter.fromMap} resolves a nested record's own bad fields to
     * null internally without failing the outer field, so the outer field itself is a well-formed (non-null)
     * record that can still contain an invalid null deeper down.
     */
    private static String firstNonNullableFieldHoldingNull(GenericData.Record datum) {
        return firstNonNullableFieldHoldingNull(datum, null);
    }

    private static String firstNonNullableFieldHoldingNull(GenericData.Record datum, String parentFieldName) {
        for (org.apache.avro.Schema.Field field : datum.getSchema().getFields()) {
            String currentFieldName = parentFieldName != null ? parentFieldName + "." + field.name() : field.name();
            Object value = datum.get(field.name());
            if (value == null) {
                if (!acceptsNull(field.schema())) {
                    return currentFieldName;
                }
            } else if (value instanceof GenericData.Record nested) {
                String nestedInvalidField = firstNonNullableFieldHoldingNull(nested, currentFieldName);
                if (nestedInvalidField != null) {
                    return nestedInvalidField;
                }
            }
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
