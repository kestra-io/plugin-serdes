package io.kestra.plugin.serdes.avro.infer;

import io.kestra.core.serializers.FileSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.*;

public class InferAvroSchema {
    public static final String NULL_DEFAULT_DESCRIPTION = "";

    private final boolean deepSearch = true;
    private int numberOfRowToScan = 100;

    private final Map<String, Field> knownFields = new HashMap<>();

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
     * @param output      where the resulting Avro schema will be written
     * @throws IllegalStateException if the input stream is empty or contains no valid records
     */
    public void inferAvroSchemaFromIon(Reader inputStream, OutputStream output) {
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
            throw new RuntimeException("could not parse Ion input stream, err: " + e.getMessage(), e);
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
            for (Map.Entry<String, Object> field : map.entrySet()) {
                inferredFields.add(
                        inferField(
                                fieldFullPath + "_" + fieldName + "_" + field.getKey(),
                                field.getKey(),
                                field.getValue()
                        )
                );
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
                    inferredField = new Field(
                        fieldName,
                        Schema.createArray(inferredType.schema()),
                        NULL_DEFAULT_DESCRIPTION,
                        Collections.emptyList()
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
        } else if (node instanceof byte[]) {  // primitive types
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
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof LocalDate || node instanceof Date) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
        } else if (node instanceof LocalTime || node instanceof OffsetTime) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))), NULL_DEFAULT_DESCRIPTION, NULL_DEFAULT_VALUE);
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
        var allCommonField = Stream.concat(a.schema().getFields().stream().map(Field::name), b.schema().getFields().stream().map(Field::name)).collect(Collectors.toCollection(LinkedHashSet::new));
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
                        // recreate them to reset the position and avoid and error
                        mergedFields.stream().map(field -> new Field(field.name(), field.schema())).collect(Collectors.toList())
                )
        );
    }
}