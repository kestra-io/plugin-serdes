package io.kestra.plugin.serdes.avro;

import io.kestra.core.serializers.FileSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

public class InferAvroSchema {
    private final boolean deepSearch = true;

    final Map<String, Field> knownFields = new HashMap<>();

    public void inferAvroSchemaFromIon(Reader inputStream, OutputStream output) {
        var foundFields = new ArrayList<Field>();
        Flux<Object> flowable = null;
        try {
            flowable = FileSerde.readAll(inputStream)
                    .take(1)// TODO see for better inference algo, since the user wants to do an API call > JSON > Parquet
                    .doOnNext(row -> {
                        foundFields.add(
                                inferField(".", "root", row)// TODO name
                        );
                    });
        } catch (IOException e) {
            throw new RuntimeException("could not parse Ion input stream, err: " + e.getMessage(), e);
        }
        flowable.count().block();
        try {
            output.write(foundFields.stream().findFirst().get().schema().toString().getBytes(StandardCharsets.UTF_8));
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
            inferredField = new Field(
                    fieldName,
                    Schema.createRecord(
                            fieldName,
                            null,
                            "io.kestra.plugin.serdes.avro",
                            false,
                            inferredFields
                    )
            );
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
                inferredField = new Field(
                        fieldName,
                        Schema.createArray(inferredType.schema())
                );
            }
            // TODO handle this case
        } else if (node instanceof byte[]) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
        } else if (node instanceof String) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        } else if (node instanceof Integer) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
        } else if (node instanceof Float) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
        } else if (node instanceof Double) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)));
        } else if (node instanceof Boolean) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)));
        } else if (
                Stream.of(Instant.class, ZonedDateTime.class, LocalDateTime.class, OffsetDateTime.class)
                        .anyMatch(c -> c.isInstance(node))
        ) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
        } else if (node instanceof LocalDate || node instanceof Date) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))));
        } else if (node instanceof LocalTime || node instanceof OffsetTime) {
            inferredField = new Field(fieldName, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))));
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
        // TODO generate ion file with kestra, and check if we always output the same structure per line -> do we remove null or empty fields/arrays ?
        // TODO check if most kestra generated ion are handled
    }

    /**
     * merge two Avro Field types, trying to output the most precise type possible
     *
     * @return the merge Avro Field, same as input if both inputs are relatively equals
     */
    public static Field mergeTypes(Field a, Field b) {

        if (a.schema().getType() == UNION || b.schema().getType() == UNION) {
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

    private static Field mergeTwoRecords(Field a, Field b) {
        var mergedFields = new ArrayList<Field>();
        var allCommonField = Stream.concat(a.schema().getFields().stream().map(Field::name), b.schema().getFields().stream().map(Field::name)).collect(Collectors.toSet());
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