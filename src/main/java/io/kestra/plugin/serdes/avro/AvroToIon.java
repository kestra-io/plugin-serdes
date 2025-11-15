package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.OnBadLines;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import java.util.Collection;

import java.io.*;
import java.net.URI;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an Avro file into ION with configurable error handling."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an Avro file to the Amazon Ion format.",
            code = """
                id: avro_to_ion
                namespace: company.team
                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/avro/products.avro
                  - id: to_ion
                    type: io.kestra.plugin.serdes.avro.AvroToIon
                    from: "{{ outputs.http_download.uri }}"
                    onBadLines: WARN
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.avro.AvroReader"
)
public class AvroToIon extends Task implements RunnableTask<AvroToIon.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "How to handle bad records (e.g., null values in non-nullable fields or type mismatches)."
    )
    private final Property<OnBadLines> onBadLines = Property.ofValue(OnBadLines.ERROR);

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        OnBadLines rOnBadLinesValue = runContext.render(this.onBadLines).as(OnBadLines.class).orElse(OnBadLines.ERROR);

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try (
            InputStream in = runContext.storage().getFile(rFrom);
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(in, datumReader);
            BufferedWriter output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            org.apache.avro.Schema avroSchema = dataFileStream.getSchema();

           Flux<GenericRecord> flowable = Flux
    .create(
        throwConsumer((FluxSink<GenericRecord> s) ->
            nextRow(dataFileStream, avroSchema, rOnBadLinesValue, runContext, s)
        ),
        FluxSink.OverflowStrategy.BUFFER
    )
    .onErrorResume(Exception.class, e -> {
        if (rOnBadLinesValue == OnBadLines.ERROR) {
            return Flux.error(new IllegalCellConversion("Bad Avro record encountered: " + e.getMessage(), e));
        } else if (rOnBadLinesValue == OnBadLines.WARN) {
            runContext.logger().warn("Bad Avro record encountered (skipped): {}", e.getMessage());
        }
        return Flux.<GenericRecord>empty();
    });

            Flux<Map<String, Object>> deserialized = flowable.map(record -> {
                try {
                    return AvroDeserializer.recordDeserializer(record);
                } catch (Exception e) {
                    if (rOnBadLinesValue == OnBadLines.ERROR) {
                        throw new IllegalCellConversion("Deserialization error for record: " + e.getMessage(), e);
                    } else if (rOnBadLinesValue == OnBadLines.WARN) {
                        runContext.logger().warn("Deserialization error (skipped): {}", e.getMessage());
                    }
                    return null;
                }
            }).filter(record -> record != null);

            Mono<Long> count = FileSerde.writeAll(output, deserialized);

            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount != null ? lineCount : 0));

            output.flush();
        }

        return new Output(runContext.storage().putFile(tempFile));
    }

    private void nextRow(DataFileStream<GenericRecord> dataFileStream, org.apache.avro.Schema avroSchema, OnBadLines onBadLinesValue, RunContext runContext, FluxSink<GenericRecord> sink) throws IOException {
        GenericRecord record = null;
        while (dataFileStream.hasNext()) {
            try {
                record = dataFileStream.next(record);

                for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                    String fieldName = field.name();
                    Object value = record.get(fieldName);
                    org.apache.avro.Schema fieldSchema = field.schema();

                    if (value == null && !isNullable(fieldSchema)) {
                        String message = "Null value for non-nullable field '" + fieldName + "' of type " + fieldSchema.getType();
                        if (onBadLinesValue == OnBadLines.ERROR) {
                            throw new IllegalCellConversion(message);
                        } else if (onBadLinesValue == OnBadLines.WARN) {
                            runContext.logger().warn(message);
                            return;
                        }
                        return;
                    }

                    try {
                        validateFieldType(value, fieldSchema, fieldName, onBadLinesValue, runContext);
                    } catch (IllegalCellConversion e) {
                        if (onBadLinesValue == OnBadLines.ERROR) {
                            throw e;
                        } else if (onBadLinesValue == OnBadLines.WARN) {
                            runContext.logger().warn(e.getMessage());
                            return;
                        }
                        return;
                    }
                }

                sink.next(record);
            } catch (org.apache.avro.AvroRuntimeException e) {
                String message = "Failed to read Avro record: " + e.getMessage();
                if (onBadLinesValue == OnBadLines.ERROR) {
                    throw new IllegalCellConversion(message, e);
                } else if (onBadLinesValue == OnBadLines.WARN) {
                    runContext.logger().warn(message);
                }
            }
        }
        sink.complete();
    }
    private void validateFieldType(Object value, org.apache.avro.Schema fieldSchema, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value == null) {
            if (isNullable(fieldSchema)) {
                return; // safe
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn("Null value for non-nullable field '{}'", fieldName);
                return;
            } else if (onBadLinesValue == OnBadLines.SKIP) {
                return;
            } else {
                throw new IllegalCellConversion("Null value for non-nullable field '" + fieldName + "'");
            }
        }

        org.apache.avro.Schema.Type type = fieldSchema.getType();
        switch (type) {
            case INT:
                validateInt(value, fieldName, onBadLinesValue, runContext);
                break;
            case LONG:
                validateLong(value, fieldName, onBadLinesValue, runContext);
                break;
            case FLOAT:
                validateFloat(value, fieldName, onBadLinesValue, runContext);
                break;
            case DOUBLE:
                validateDouble(value, fieldName, onBadLinesValue, runContext);
                break;
            case STRING:
                validateString(value, fieldName, onBadLinesValue, runContext);
                break;
            case BOOLEAN:
                validateBoolean(value, fieldName, onBadLinesValue, runContext);
                break;
            case UNION:
                validateUnion(value, fieldSchema, fieldName, onBadLinesValue, runContext);
                break;
            case ARRAY:
                validateArray(value, fieldSchema, fieldName, onBadLinesValue, runContext);
                break;
            case MAP:
                validateMap(value, fieldSchema, fieldName, onBadLinesValue, runContext);
                break;
            case RECORD:
                validateRecord(value, fieldSchema, fieldName, onBadLinesValue, runContext);
                break;
            case NULL:
                validateNull(value, fieldName, onBadLinesValue, runContext);
                break;
            default:
                throw new IllegalCellConversion("Unsupported Avro type for field '" + fieldName + "': " + type);
        }
    }

    private void validateInt(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String) {
            try {
                Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalCellConversion("Invalid INT value for field '" + fieldName + "': " + value);
            }
        } else if (!(value instanceof Integer)) {
            throw new IllegalCellConversion("Invalid type for field '" + fieldName + "': expected INT, got " + value.getClass().getSimpleName());
        }
    }

    private void validateLong(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String) {
            try {
                Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalCellConversion("Invalid LONG value for field '" + fieldName + "': " + value);
            }
        } else if (!(value instanceof Long)) {
            throw new IllegalCellConversion("Invalid type for field '" + fieldName + "': expected LONG, got " + value.getClass().getSimpleName());
        }
    }

    private void validateFloat(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String) {
            try {
                Float.parseFloat((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalCellConversion("Invalid FLOAT value for field '" + fieldName + "': " + value);
            }
        } else if (!(value instanceof Float)) {
            throw new IllegalCellConversion("Invalid type for field '" + fieldName + "': expected FLOAT, got " + value.getClass().getSimpleName());
        }
    }

    private void validateDouble(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String) {
            try {
                Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalCellConversion("Invalid DOUBLE value for field '" + fieldName + "': " + value);
            }
        } else if (!(value instanceof Double)) {
            throw new IllegalCellConversion("Invalid type for field '" + fieldName + "': expected DOUBLE, got " + value.getClass().getSimpleName());
        }
    }

    private void validateString(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String || value instanceof org.apache.avro.util.Utf8) {
            // Convert Utf8 to String for consistency
            if (value instanceof org.apache.avro.util.Utf8) {
                value = value.toString();
            }
        } else {
            String message = "Invalid type for field '" + fieldName + "': expected STRING, got " + (value != null ? value.getClass().getSimpleName() : "null");
            if (onBadLinesValue == OnBadLines.ERROR) {
                throw new IllegalCellConversion(message);
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn(message);
            }
            return; // SKIP or WARN: continue
        }
    }

    private void validateBoolean(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value instanceof String) {
            try {
                Boolean.parseBoolean((String) value);
            } catch (Exception e) {
                throw new IllegalCellConversion("Invalid BOOLEAN value for field '" + fieldName + "': " + value);
            }
        } else if (!(value instanceof Boolean)) {
            throw new IllegalCellConversion("Invalid type for field '" + fieldName + "': expected BOOLEAN, got " + value.getClass().getSimpleName());
        }
    }

    private void validateUnion(Object value, org.apache.avro.Schema fieldSchema, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        boolean valid = false;
        for (org.apache.avro.Schema unionType : fieldSchema.getTypes()) {
            try {
                validateFieldType(value, unionType, fieldName, onBadLinesValue, runContext);
                valid = true;
                break;
            } catch (IllegalCellConversion ignored) {
                // Continue to next union type
            }
        }
        if (!valid) {
            throw new IllegalCellConversion("No matching union type for field '" + fieldName + "': value " + value);
        }
    }

    private void validateArray(Object value, org.apache.avro.Schema fieldSchema, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (!(value instanceof Collection)) {
            String message = "Invalid type for field '" + fieldName + "': expected ARRAY, got " + value.getClass().getSimpleName();
            if (onBadLinesValue == OnBadLines.ERROR) {
                throw new IllegalCellConversion(message);
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn(message);
            }
            return; // Skip validation for this field
        }
        Collection<?> arrayValue = (Collection<?>) value;
        org.apache.avro.Schema elementSchema = fieldSchema.getElementType();
        int index = 0;
        for (Object item : arrayValue) {
            validateFieldType(item, elementSchema, fieldName + "[" + index + "]", onBadLinesValue, runContext);
            index++;
        }
    }

    private void validateMap(Object value, org.apache.avro.Schema fieldSchema, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (!(value instanceof Map)) {
            String message = "Invalid type for field '" + fieldName + "': expected MAP, got " + value.getClass().getSimpleName();
            if (onBadLinesValue == OnBadLines.ERROR) {
                throw new IllegalCellConversion(message);
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn(message);
            }
            return;
        }
        @SuppressWarnings("unchecked")
        Map<Object, Object> mapValue = (Map<Object, Object>) value;
        org.apache.avro.Schema valueSchema = fieldSchema.getValueType();
        for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
            if (!(entry.getKey() instanceof CharSequence)) {
                String message = "Invalid key type for map field '" + fieldName + "': expected STRING, got " + entry.getKey().getClass().getSimpleName();
                if (onBadLinesValue == OnBadLines.ERROR) {
                    throw new IllegalCellConversion(message);
                } else if (onBadLinesValue == OnBadLines.WARN) {
                    runContext.logger().warn(message);
                }
                return;
            }
            validateFieldType(entry.getValue(), valueSchema, fieldName + "[" + entry.getKey() + "]", onBadLinesValue, runContext);
        }
    }

    private void validateRecord(Object value, org.apache.avro.Schema fieldSchema, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (!(value instanceof GenericRecord)) {
            String message = "Invalid type for field '" + fieldName + "': expected RECORD, got " + value.getClass().getSimpleName();
            if (onBadLinesValue == OnBadLines.ERROR) {
                throw new IllegalCellConversion(message);
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn(message);
            }
            return;
        }
        GenericRecord recordValue = (GenericRecord) value;
        for (org.apache.avro.Schema.Field subField : fieldSchema.getFields()) {
            Object subValue = recordValue.get(subField.name());
            validateFieldType(subValue, subField.schema(), fieldName + "." + subField.name(), onBadLinesValue, runContext);
        }
    }

    private void validateNull(Object value, String fieldName, OnBadLines onBadLinesValue, RunContext runContext) {
        if (value != null) {
            String message = "Non-null value for null-typed field '" + fieldName + "': " + value.getClass().getSimpleName();
            if (onBadLinesValue == OnBadLines.ERROR) {
                throw new IllegalCellConversion(message);
            } else if (onBadLinesValue == OnBadLines.WARN) {
                runContext.logger().warn(message);
            }
            // For SKIP or WARN, continue (effectively ignore the invalid value)
        }
    }

    private boolean isNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.NULL) {
            return true;
        }
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream().anyMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }

    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the output Ion file")
        private URI uri;

        public Output(URI uri) {
            this.uri = uri;
        }
    }

    public static class IllegalCellConversion extends RuntimeException {
        public IllegalCellConversion(String message) {
            super(message);
        }

        public IllegalCellConversion(String message, Throwable cause) {
            super(message, cause);
        }
    }
}