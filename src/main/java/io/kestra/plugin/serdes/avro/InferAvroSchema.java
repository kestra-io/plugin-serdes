package io.kestra.plugin.serdes.avro;

import io.kestra.core.serializers.FileSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class InferAvroSchema {
    private InferAvroSchema() {
    }

    public static void inferAvroSchemaFromIon(Reader inputStream, OutputStream output) {
        var foundFields = new ArrayList<Schema.Field>();
        Flux<Object> flowable = null;
        try {
            flowable = FileSerde.readAll(inputStream)
                    .take(1)// TODO see for better inference algo, since the user wants to do an API call > JSON > Parquet
                    .doOnNext(row -> {
                        foundFields.add(
                                inferField("root_name_to_name", row)// TODO name
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
    private static Schema.Field inferField(String name, Object node) {
        if (node instanceof Map) {
            var map = (Map<String, Object>) node;
            var inferredFields = new ArrayList<Schema.Field>();
            for (Map.Entry<String, Object> field : map.entrySet()) {
                inferredFields.add(
                        inferField(
                                field.getKey(),
                                field.getValue()
                        )
                );
            }
            return new Schema.Field(
                    name,
                    Schema.createRecord(
                            name,
                            null,
                            "io.kestra.plugin.serdes.avro",
                            false,
                            inferredFields
                    )
            );
        } else if (node instanceof List) {
            var list = (List<Object>) node;
            if (!list.isEmpty()) {
                var inferredType = inferField(name + "_items", list.get(0));
                return new Schema.Field(
                        name,
                        Schema.createArray(inferredType.schema())
                );
            }
            // TODO handle this case
        } else if (node instanceof byte[]) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
        } else if (node instanceof String) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        } else if (node instanceof Integer) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
        } else if (node instanceof Float) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
        } else if (node instanceof Double) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)));
        } else if (node instanceof Boolean) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)));
        } else if (
                Stream.of(Instant.class, ZonedDateTime.class, LocalDateTime.class, OffsetDateTime.class)
                        .anyMatch(c -> c.isInstance(node))
        ) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
        } else if (node instanceof LocalDate || node instanceof Date) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))));
        } else if (node instanceof LocalTime || node instanceof OffsetTime) {
            return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))));
        }

        // TODO add unit test with complex nested structure, with clashing names (generate fields name ? UUID ?)
        // TODO generate ion file with kestra, and check if we always output the same structure per line -> do we remove null or empty fields/arrays ?
        // TODO check if most kestra generated ion are handled

        throw new IllegalArgumentException("Unhandled node " + name + " with content: " + node);
    }
}