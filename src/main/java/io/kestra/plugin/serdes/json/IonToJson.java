package io.kestra.plugin.serdes.json;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.ion.IonFactory;

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
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an ION file into a JSONL file",
    description = """
        Outputs one JSON line per ION record by default; set `newLine` to false \
        to produce a JSON array instead. ION timestamps use the configured \
        `timeZoneId`. Set `shouldKeepAnnotations` to true to preserve ION \
        type annotations as `ion_annotations` fields in the output."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an ION file to newline-delimited JSON (JSONL).",
            code = """
                id: ion_to_json
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.http_download.uri }}"

                  - id: to_json
                    type: io.kestra.plugin.serdes.json.IonToJson
                    from: "{{ outputs.convert.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.json.JsonWriter"
)
public class IonToJson extends Task implements RunnableTask<IonToJson.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty(group = "processing")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(
        title = "Whether the file is newline-delimited JSON (JSONL)",
        description = "Whether the file uses newline-delimited JSON.\n" +
            "Warning: if not, the whole file will be loaded into memory and can lead to out-of-memory errors."
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> newLine = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Timezone to use when rendering timestamps",
        description = """
            All ION timestamp values are rendered using this timezone, even if the \
            source timestamp carries its own offset. Defaults to the system \
            timezone."""
    )
    @PluginProperty(group = "execution")
    private final Property<String> timeZoneId = Property.ofValue(ZoneId.systemDefault().toString());

    @Builder.Default
    @Schema(
        title = "Should keep ION annotations in the output JSON",
        description = "If true, ION annotations will be preserved in the output JSON. Default is false."
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> shouldKeepAnnotations = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        boolean isNewLine = runContext.render(this.newLine).as(Boolean.class).orElse(true);
        var suffix = isNewLine ? ".jsonl" : ".json";
        var tempFile = runContext.workingDir().createTempFile(suffix).toFile();

        var outputCharset = Charset.forName(runContext.render(this.charset).as(String.class).orElse(StandardCharsets.UTF_8.name()));

        var zoneId = ZoneId.of(runContext.render(this.timeZoneId).as(String.class).orElse(ZoneId.systemDefault().toString()));

        // Default ION path yields raw embedded Timestamp objects and NON_NULL inclusion, neither handled by a plain JSON mapper.
        var ionTimestampModule = new SimpleModule()
            .addSerializer(Timestamp.class, new JsonSerializer<Timestamp>() {
                @Override
                public void serialize(Timestamp value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                    gen.writeString(formatTimestamp(toInstant(value), zoneId));
                }
            });

        // No .setTimeZone(...): ION's only temporal type is rendered by ionTimestampModule via the closed-over zoneId, not the mapper's TimeZone.
        var jsonObjectMapper = JacksonMapper.ofJson().copy()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS)
            .registerModule(ionTimestampModule);

        var rKeepAnnotations = runContext.render(this.shouldKeepAnnotations).as(Boolean.class).orElse(false);
        Long recordCount = null;

        try (
            InputStream inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE);
            Writer fileWriter = new BufferedWriter(new FileWriter(tempFile, outputCharset), FileSerde.BUFFER_SIZE);
            JsonGenerator jsonGenerator = jsonObjectMapper.createGenerator(fileWriter)
        ) {
            Flux<Object> flowable;

            if (!rKeepAnnotations) {
                var ionFactory = new IonFactory(jsonObjectMapper);
                var ionParser = ionFactory.createParser(inputStream);

                if (isNewLine) {
                    flowable = Flux.generate(
                        () -> ionParser,
                        (parser, sink) ->
                        {
                            try {
                                if (parser.nextToken() != null) {
                                    var row = jsonObjectMapper.readValue(parser, Object.class);
                                    jsonGenerator.writeObject(row);
                                    jsonGenerator.flush();
                                    fileWriter.write("\n");
                                    sink.next(new Object());
                                } else {
                                    parser.close();
                                    sink.complete();
                                }
                            } catch (Exception e) {
                                sink.error(e);
                            }
                            return parser;
                        }
                    );
                } else {
                    flowable = Flux.generate(
                        () -> new Object[] { ionParser, new Object[1], new boolean[] { false }, new boolean[] { false } },
                        (state, sink) ->
                        {
                            var parser = (com.fasterxml.jackson.core.JsonParser) state[0];
                            var firstRow = (Object[]) state[1];
                            var isFirst = (boolean[]) state[2];
                            var hasMultiple = (boolean[]) state[3];

                            try {
                                if (!isFirst[0]) {
                                    if (parser.nextToken() != null) {
                                        firstRow[0] = jsonObjectMapper.readValue(parser, Object.class);
                                        isFirst[0] = true;
                                        sink.next(new Object());
                                    } else {
                                        parser.close();
                                        sink.complete();
                                    }
                                } else if (parser.nextToken() != null) {
                                    if (!hasMultiple[0]) {
                                        hasMultiple[0] = true;
                                        jsonGenerator.writeStartArray();
                                        jsonGenerator.writeObject(firstRow[0]);
                                    }
                                    var row = jsonObjectMapper.readValue(parser, Object.class);
                                    jsonGenerator.writeObject(row);
                                    sink.next(new Object());
                                } else {
                                    if (!hasMultiple[0]) {
                                        jsonGenerator.writeObject(firstRow[0]);
                                    } else {
                                        jsonGenerator.writeEndArray();
                                    }
                                    parser.close();
                                    sink.complete();
                                }
                            } catch (Exception e) {
                                sink.error(e);
                            }
                            return state;
                        }
                    );
                }
            } else {
                var ionSystem = IonSystemBuilder.standard().build();
                var ionReader = ionSystem.newReader(runContext.storage().getFile(from));

                if (isNewLine) {
                    flowable = Flux.generate(
                        () -> ionReader,
                        (reader, sink) ->
                        {
                            try {
                                IonType type = reader.next();
                                if (type != null) {
                                    var value = ionSystem.newValue(reader);
                                    writeIonValueWithAnnotations(jsonObjectMapper, jsonGenerator, value, zoneId, "root");
                                    jsonGenerator.flush();
                                    fileWriter.write("\n");
                                    sink.next(new Object());
                                } else {
                                    reader.close();
                                    sink.complete();
                                }
                            } catch (Exception e) {
                                sink.error(e);
                            }
                            return reader;
                        }
                    );
                } else {
                    flowable = Flux.generate(
                        () -> new Object[] { ionReader, new IonValue[1], new boolean[] { false }, new boolean[] { false } },
                        (state, sink) ->
                        {
                            var reader = (IonReader) state[0];
                            var firstValue = (IonValue[]) state[1];
                            var isFirst = (boolean[]) state[2];
                            var hasMultiple = (boolean[]) state[3];

                            try {
                                if (!isFirst[0]) {
                                    IonType firstType = reader.next();
                                    if (firstType != null) {
                                        firstValue[0] = ionSystem.newValue(reader);
                                        isFirst[0] = true;
                                        sink.next(new Object());
                                    } else {
                                        reader.close();
                                        sink.complete();
                                    }
                                } else if (reader.next() != null) {
                                    if (!hasMultiple[0]) {
                                        hasMultiple[0] = true;
                                        jsonGenerator.writeStartArray();
                                        writeIonValueWithAnnotations(jsonObjectMapper, jsonGenerator, firstValue[0], zoneId, "root");
                                    }
                                    var value = ionSystem.newValue(reader);
                                    writeIonValueWithAnnotations(jsonObjectMapper, jsonGenerator, value, zoneId, "root");
                                    sink.next(new Object());
                                } else {
                                    if (!hasMultiple[0]) {
                                        writeIonValueWithAnnotations(jsonObjectMapper, jsonGenerator, firstValue[0], zoneId, "root");
                                    } else {
                                        jsonGenerator.writeEndArray();
                                    }
                                    reader.close();
                                    sink.complete();
                                }
                            } catch (Exception e) {
                                sink.error(e);
                            }
                            return state;
                        }
                    );
                }
            }

            Mono<Long> count = flowable.count();
            recordCount = count.block();

            runContext.metric(Counter.of("records", recordCount));
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(recordCount != null ? recordCount : 0L)
            .build();
    }

    // com.amazon.ion.Timestamp.dateValue() truncates to millisecond precision; build the Instant from the
    // Z-normalized (UTC) fields directly so fractional seconds beyond milliseconds are preserved. Rounding a
    // >9-digit fraction (e.g. .9999999999) up can overflow to 1_000_000_000 nanos: Instant.ofEpochSecond's
    // nanoAdjustment is unrestricted and carries the overflow into the next second instead of throwing.
    private static Instant toInstant(Timestamp timestamp) {
        var decimalSecond = timestamp.getZDecimalSecond();
        var wholeSeconds = decimalSecond.intValue();
        var epochSecond = LocalDateTime.of(
            timestamp.getZYear(), timestamp.getZMonth(), timestamp.getZDay(),
            timestamp.getZHour(), timestamp.getZMinute(), wholeSeconds
        ).toEpochSecond(ZoneOffset.UTC);
        var nanos = decimalSecond.subtract(BigDecimal.valueOf(wholeSeconds))
            .movePointRight(9)
            .setScale(0, RoundingMode.HALF_UP)
            .longValueExact();

        return Instant.ofEpochSecond(epochSecond, nanos);
    }

    private static String formatTimestamp(Instant instant, ZoneId zoneId) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(instant.atZone(zoneId));
    }

    private void writeIonValueWithAnnotations(ObjectMapper mapper, JsonGenerator jsonGenerator, IonValue value, ZoneId zoneId, String parentFieldName) throws IOException {
        var type = value.getType();

        if (value.isNullValue()) {
            jsonGenerator.writeNull();
            return;
        }

        var annotations = value.getTypeAnnotations();

        if ((type == IonType.STRING || type == IonType.SYMBOL) && annotations.length > 0) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("ion_annotations");
            jsonGenerator.writeStartArray();
            for (var ann : annotations) {
                jsonGenerator.writeString(ann);
            }
            jsonGenerator.writeEndArray();
            jsonGenerator.writeFieldName("value");
            jsonGenerator.writeString(((IonText) value).stringValue());
            jsonGenerator.writeEndObject();
            return;
        }

        switch (type) {
            case STRUCT -> {
                IonStruct struct = (IonStruct) value;

                jsonGenerator.writeStartObject();
                for (var child : struct) {
                    var fieldName = child.getFieldName();
                    jsonGenerator.writeFieldName(fieldName);
                    writeIonValueWithAnnotations(mapper, jsonGenerator, child, zoneId, fieldName);
                }
                jsonGenerator.writeEndObject();
            }

            case LIST -> {
                jsonGenerator.writeStartArray();
                var list = (IonList) value;
                for (IonValue v : list) {
                    writeIonValueWithAnnotations(mapper, jsonGenerator, v, zoneId, null);
                }
                jsonGenerator.writeEndArray();
            }
            case SEXP -> {
                // treat S-expression like an array
                jsonGenerator.writeStartArray();
                var sexp = (IonSexp) value;
                for (var v : sexp) {
                    writeIonValueWithAnnotations(mapper, jsonGenerator, v, zoneId, null);
                }
                jsonGenerator.writeEndArray();
            }
            case BOOL -> jsonGenerator.writeBoolean(((IonBool) value).booleanValue());
            case INT -> {
                var ionInt = (IonInt) value;
                switch (ionInt.getIntegerSize()) {
                    case INT -> jsonGenerator.writeNumber(ionInt.intValue());
                    case LONG -> jsonGenerator.writeNumber(ionInt.longValue());
                    case BIG_INTEGER -> jsonGenerator.writeNumber(ionInt.bigIntegerValue());
                }
            }
            case FLOAT -> jsonGenerator.writeNumber(((IonFloat) value).doubleValue());
            case DECIMAL -> jsonGenerator.writeNumber(((IonDecimal) value).decimalValue());
            case TIMESTAMP -> {
                var ionTimestamp = ((IonTimestamp) value).timestampValue();
                jsonGenerator.writeString(formatTimestamp(toInstant(ionTimestamp), zoneId));
            }
            case STRING, SYMBOL -> {
                var text = ((IonText) value).stringValue();

                if (parentFieldName != null) {
                    switch (parentFieldName) {
                        case "enum", "nameNullable" -> {
                            jsonGenerator.writeString(text);
                            return;
                        }
                        case "date" -> {
                            var formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                            var parsed = LocalDate.parse(text, formatter);
                            jsonGenerator.writeString(formatter.format(parsed));
                            return;
                        }
                        case "timeMillis" -> {
                            var formatter = DateTimeFormatter.ofPattern("H:mm");
                            var parsed = LocalTime.parse(text, formatter);
                            jsonGenerator.writeString(formatter.format(parsed));
                            return;
                        }
                    }
                }

                jsonGenerator.writeString(text);
            }
            case BLOB, CLOB -> jsonGenerator.writeBinary(((IonLob) value).getBytes());
            default -> {
                var out = new ByteArrayOutputStream();
                var ionWriter = IonTextWriterBuilder.json().build(out);
                value.writeTo(ionWriter);
                ionWriter.close();
                var node = mapper.readTree(new ByteArrayInputStream(out.toByteArray()));
                mapper.writeTree(jsonGenerator, node);
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;

        @Schema(title = "The number of records converted")
        private long size;
    }
}
