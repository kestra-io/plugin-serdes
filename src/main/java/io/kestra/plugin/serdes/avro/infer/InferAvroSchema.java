package io.kestra.plugin.serdes.avro.infer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.kestra.core.serializers.FileSerde;

import reactor.core.publisher.Mono;

import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.*;

public class InferAvroSchema {
    public static final String NULL_DEFAULT_DESCRIPTION = "";

    private static final Pattern INVALID_NAME_CHARS = Pattern.compile("[^A-Za-z0-9_]");

    private final boolean deepSearch = true;
    private int numberOfRowToScan = 100;

    private final Map<String, Field> knownFields = new HashMap<>();

    // Stable mapping from fieldFullPath → assigned sanitized name, so that the
    // same original key always gets the same Avro name regardless of row order.
    private final Map<String, String> assignedNames = new HashMap<>();

    public InferAvroSchema() {
    }

    public InferAvroSchema(int numberOfRowToScan) {
        this.numberOfRowToScan = numberOfRowToScan;
        if (numberOfRowToScan < 1) {
            throw new IllegalArgumentException("Number of rows to scan must be greater than 0");
        }
    }

    /**
     * infer an Avro schema from a Ion source
     *
     * @param inputStream Ion source
     * @param output where the resulting Avro schema will be written
     * @throws IllegalStateException if the input stream is empty or contains no valid records
     */
    public void inferAvroSchemaFromIon(InputStream inputStream, OutputStream output) {
        Mono<Schema> inferedSchema = null;
        try {
            inferedSchema = FileSerde.readAll(inputStream)
                .take(numberOfRowToScan)
                .map(row -> inferField(".", "root", row))
                .reduce(
                    InferAvroSchema::mergeTypes
                )
                .map(Field::schema);
        } catch (IOException e) {
            throw new RuntimeException("could not parse ION input stream, err: " + e.getMessage(), e);
        }
        try {
            Schema schema = inferedSchema.block();
            if (schema == null) {
                throw new IllegalStateException("Cannot infer Avro schema from ION input: the file appears to be empty or contains no valid records.");
            }
            output.write(schema.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException("could not write Avro schema in output stream, err: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Field inferField(String fieldFullPath, String fieldName, Object node) {
        Field inferredField = null;
        if (node instanceof Map) {
            var map = (Map<String, Object>) node;
            var inferredFields = new ArrayList<Field>();

            // Two passes: first reuse any previously assigned names (stable across rows),
            // then assign new names for keys we haven't seen before.
            var usedNames = new LinkedHashSet<String>();
            var keyToPath = new LinkedHashMap<String, String>();
            for (var key : map.keySet()) {
                var path = fieldFullPath + "_" + fieldName + "_" + key;
                keyToPath.put(key, path);
                var existing = assignedNames.get(path);
                if (existing != null) {
                    usedNames.add(existing);
                }
            }
            for (var key : map.keySet()) {
                var path = keyToPath.get(key);
                if (!assignedNames.containsKey(path)) {
                    var sanitized = deduplicateFieldName(sanitizeFieldName(key), usedNames);
                    usedNames.add(sanitized);
                    assignedNames.put(path, sanitized);
                }
            }

            for (var entry : map.entrySet()) {
                var path = keyToPath.get(entry.getKey());
                var sanitized = assignedNames.get(path);
                var field = inferField(path, sanitized, entry.getValue());
                // Register the original key as an alias so AvroConverter can look up
                // the value from the Ion row even after the field has been renamed.
                if (!entry.getKey().equals(sanitized) && !field.aliases().contains(entry.getKey())) {
                    field.addAlias(entry.getKey());
                }
                inferredFields.add(field);
            }

            var recordSchema = Schema.createRecord(
                fieldName,
                null,
                "io.kestra.plugin.serdes.avro",
                false,
                inferredFields
            );
            if ("root".equals(fieldName)) {
                inferredField = new Field(
                    fieldName,
                    recordSchema
                );
            } else {
                inferredField = new Field(
                    fieldName,
                    Schema.createUnion(
                        recordSchema,
                        Schema.create(Schema.Type.NULL)
                    )
                );
            }
        } else if (node instanceof List) {
            var list = (List<Object>) node;
            if (!list.isEmpty()) {
                Field inferredType = null;
                if (deepSearch) {
                    for (Object item : list) {
                        inferredType = inferField(fieldFullPath + "_" + fieldName + "_items", fieldName + "_items", item);
                    }
                } else {
                    inferredType = inferField(fieldFullPath + "_" + fieldName + "_items", fieldName + "_items", list.get(0));
                }
                if ("root".equals(fieldName)) {
                    // Avro requires a RECORD at the top level. Wrap the root array in a record
                    // with a single "value" field so the schema is valid for DataFileWriter.
                    var arraySchema = Schema.createArray(inferredType.schema());
                    var valueField = new Field("value", arraySchema, NULL_DEFAULT_DESCRIPTION, Collections.emptyList());
                    inferredField = new Field(
                        fieldName,
                        Schema.createRecord(fieldName, null, "io.kestra.plugin.serdes.avro", false, List.of(valueField))
                    );
                } else {
                    inferredField = new Field(
                        fieldName,
                        Schema.createUnion(
                            Schema.createArray(inferredType.schema()),
                            Schema.create(Schema.Type.NULL)
                        )
                    );
                }
            } else {
                inferredField = new Field(
                    fieldName,
                    Schema.createUnion(
                        Schema.createArray(Schema.create(STRING)),
                        Schema.create(Schema.Type.NULL)
                    )
                );
            }
        } else if (node instanceof byte[]) { // primitive types
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof String || node instanceof BigDecimal) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof Integer) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof Float) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof Double) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof Boolean) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (
            Stream.of(Instant.class, ZonedDateTime.class, LocalDateTime.class, OffsetDateTime.class)
                .anyMatch(c -> c.isInstance(node))
        ) {
            inferredField = new Field(
                fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))), NULL_DEFAULT_DESCRIPTION,
                NULL_DEFAULT_VALUE
            );
        } else if (node instanceof LocalDate || node instanceof Date) {
            inferredField = new Field(
                fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE
            );
        } else if (node instanceof LocalTime || node instanceof OffsetTime) {
            inferredField = new Field(
                fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))), NULL_DEFAULT_DESCRIPTION,
                NULL_DEFAULT_VALUE
            );
        } else if (node == null) {
            inferredField = new Field(fieldName, Schema.create(Schema.Type.NULL));
        }

        if (inferredField == null) {
            throw new IllegalArgumentException("Unhandled node " + fieldFullPath + " with content: " + node);
        } else {
            var knowField = knownFields.get(fieldFullPath);
            if (knowField != null) {
                var mergedField = mergeTypes(inferredField, knowField);
                knownFields.put(fieldFullPath, mergedField);
                return mergedField;
            } else {
                knownFields.put(fieldFullPath, inferredField);
                return inferredField;
            }
        }
    }

    private static String sanitizeFieldName(String fieldName) {
        var sanitized = INVALID_NAME_CHARS.matcher(fieldName).replaceAll("_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    // Append _1, _2, … when sanitization causes a collision (e.g. foo-bar and foo_bar).
    private static String deduplicateFieldName(String name, Set<String> usedNames) {
        if (!usedNames.contains(name)) {
            return name;
        }
        var counter = 1;
        while (usedNames.contains(name + "_" + counter)) {
            counter++;
        }
        return name + "_" + counter;
    }

    /**
     * merge two Avro Field types, trying to output the most precise type possible
     *
     * @return the merge Avro Field, same as input if both inputs are relatively equals
     */
    public static Field mergeTypes(Field a, Field b) {
        if (a.schema().getType() == UNION || b.schema().getType() == UNION) {
            var set = mergeAtLeastOneUnion(a, b);
            return new Field(a, Schema.createUnion(new ArrayList<>(set)));
        } else if (a.schema().getType() == RECORD || b.schema().getType() == RECORD) {
            if (a.schema().getType() == RECORD && b.schema().getType() == RECORD) {
                return mergeTwoRecords(a, b);
            } else {
                throw new IllegalArgumentException("Unhandled merging a Record with a type different than record, a:" + a.schema().getType() + ", b:" + b.schema().getType());
            }
        } else if (a.schema().getType() == b.schema().getType()) {
            return a;
        } else {
            return new Field(a, Schema.createUnion(a.schema(), b.schema()));
        }
    }

    private static LinkedHashSet<Schema> mergeAtLeastOneUnion(Field a, Field b) {
        var set = new LinkedHashSet<Schema>();
        if (a.schema().getType() == UNION) {
            set.addAll(a.schema().getTypes());
        } else {
            set.add(a.schema());
        }
        if (b.schema().getType() == UNION) {
            set.addAll(b.schema().getTypes());
        } else {
            set.add(b.schema());
        }
        var recordsToMerge = set.stream().filter(x -> RECORD.equals(x.getType())).toList();
        var arraysToMerge = set.stream().filter(x -> ARRAY.equals(x.getType())).toList();
        if (recordsToMerge.size() > 1) {
            // this will keep NULL as first type
            set = set.stream().filter(x -> !RECORD.equals(x.getType())).collect(Collectors.toCollection(LinkedHashSet::new));
            set.add(mergeTwoRecords(new Field("tmp", recordsToMerge.get(0)), new Field("tmp2", recordsToMerge.get(1))).schema());
        } else if (arraysToMerge.size() > 1) {
            set = set.stream().filter(x -> !ARRAY.equals(x.getType())).collect(Collectors.toCollection(LinkedHashSet::new));
            set.add(mergeTypes(new Field("tmp", arraysToMerge.get(0)), new Field("tmp2", arraysToMerge.get(1))).schema());
        }
        return set;
    }

    private static Field mergeTwoRecords(Field a, Field b) {
        var mergedFields = new ArrayList<Field>();
        var allCommonField = Stream.concat(a.schema().getFields().stream().map(Field::name), b.schema().getFields().stream().map(Field::name))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String commonField : allCommonField) {
            var fieldFromA = a.schema().getField(commonField);
            var fieldFromB = b.schema().getField(commonField);
            if (fieldFromA != null && fieldFromB != null) {
                mergedFields.add(
                    mergeTypes(fieldFromA, fieldFromB)
                );
            } else if (fieldFromA != null) {
                mergedFields.add(new Field(fieldFromA, fieldFromA.schema()));
            } else {
                mergedFields.add(new Field(fieldFromB, fieldFromB.schema()));
            }
        }
        return new Field(
            a,
            Schema.createRecord(
                a.schema().getName(),
                a.schema().getDoc(),
                a.schema().getNamespace(),
                false,
                // Recreate fields to reset position; carry over aliases so
                // AvroConverter can still look up values by the original Ion key.
                mergedFields.stream().map(field -> {
                    var rebuilt = new Field(field.name(), field.schema());
                    field.aliases().forEach(rebuilt::addAlias);
                    return rebuilt;
                }).collect(Collectors.toList())
            )
        );
    }
}