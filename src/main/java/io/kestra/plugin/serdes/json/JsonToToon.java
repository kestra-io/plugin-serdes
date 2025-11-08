package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a JSON file into TOON.",
    description = """
        Converts JSON data to the TOON (Token-Oriented Object Notation) format, a deterministic, indentation-based text format that encodes the JSON data model with explicit structure and minimal quoting.
        TOON is efficient for uniform arrays of objects and supports tabular encoding.
        See https://github.com/toon-format/spec for the full specification (1.4).
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a JSON file to TOON format.",
            code = """
                id: json_to_toon
                namespace: company.team

                tasks:
                  - id: create_json
                    type: io.kestra.plugin.core.storage.Write
                    extension: json
                    content: |
                      {
                        "products": [
                          {"id": 1, "name": "Apple", "price": 1.2},
                          {"id": 2, "name": "Banana", "price": 0.9}
                        ],
                        "metadata": {
                          "category": "fruits",
                          "country": "France"
                        }
                      }

                  - id: to_toon
                    type: io.kestra.plugin.serdes.json.JsonToToon
                    from: "{{ outputs.create_json.uri }}"

                  - id: summarize
                    type: io.kestra.plugin.openai.chatcompletion
                    model: gpt-4o-mini
                    messages:
                      - role: system
                        content: "You are an assistant that summarizes data."
                      - role: user
                        content: |
                          Here is a TOON representation of product data:
                          {{ outputs.to_toon.uri }}
                          Summarize the main product categories and price ranges.
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    }
)
public class JsonToToon extends Task implements RunnableTask<JsonToToon.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson().copy()
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .setSerializationInclusion(JsonInclude.Include.ALWAYS)
        .setTimeZone(TimeZone.getDefault());

    @NotNull
    @Schema(title = "Source file URI")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var tempFile = runContext.workingDir().createTempFile(".toon").toFile();
        var rCharset = runContext.render(this.charset).as(String.class).orElseThrow();

        try (
            var input = new BufferedReader(new InputStreamReader(runContext.storage().getFile(rFrom), rCharset), FileSerde.BUFFER_SIZE);
            var writer = new BufferedWriter(new FileWriter(tempFile, Charset.forName(rCharset)), FileSerde.BUFFER_SIZE)
        ) {
            var reader = OBJECT_MAPPER.readerFor(Object.class);
            var json = reader.readValue(input);

            var encoder = new ToonEncoder(writer);
            long count = encoder.encode(json);
            runContext.metric(Counter.of("records", count));
        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "URI of the resulting TOON file")
        private final URI uri;
    }

    private static class ToonEncoder {
        private final Writer writer;
        private static final int INDENT_SIZE = 2;
        private static final String INDENT_UNIT = " ".repeat(INDENT_SIZE);

        private long count = 0;

        ToonEncoder(Writer writer) {
            this.writer = writer;
        }

        public long encode(Object json) throws IOException {
            if (json == null) return 0;
            writeValue(json);
            writer.flush();
            return count;
        }

        private void writeValue(Object value) throws IOException {
            if (value instanceof Map<?, ?> map) {
                writeObject(map, 0);
            } else if (value instanceof List<?> list) {
                writeArray(list, 0, "root");
            } else {
                writer.write(quoteIfNeeded(value));
                writer.write("\n");
            }
        }

        private void writeObject(Map<?, ?> map, int indent) throws IOException {
            for (var entry : map.entrySet()) {
                var key = formatKey(entry.getKey().toString());
                var val = entry.getValue();

                if (val instanceof Map<?, ?> subMap) {
                    indent(indent);
                    writer.write(key + ":\n");
                    writeObject(subMap, indent + 1);
                } else if (val instanceof List<?> list) {
                    writeArray(list, indent, key);
                } else {
                    indent(indent);
                    writer.write(key + ": " + quoteIfNeeded(val) + "\n");
                }
            }
        }

        private void writeArray(List<?> list, int indent, String key) throws IOException {
            count += list.size();

            var allPrimitive = list.stream().allMatch(e ->
                !(e instanceof Map) && !(e instanceof List)
            );

            var uniformObject = !list.isEmpty() && list.stream().allMatch(e -> e instanceof Map);

            if (allPrimitive) {
                // Inline primitive array
                indent(indent);
                writer.write(key + "[" + list.size() + "]:");
                if (!list.isEmpty()) {
                    writer.write(" ");
                    writer.write(String.join(",", list.stream()
                        .map(this::quoteIfNeeded)
                        .toList()));
                }
                writer.write("\n");
            } else if (uniformObject) {
                // Tabular array
                @SuppressWarnings("unchecked")
                var firstRow = (Map<String, Object>) list.get(0);
                var fields = firstRow.keySet();
                indent(indent);
                writer.write(key + "[" + list.size() + "]{" + String.join(",", fields) + "}:\n");
                for (var row : list) {
                    indent(indent + 1);
                    @SuppressWarnings("unchecked")
                    var map = (Map<String, Object>) row;
                    var values = fields.stream().map(map::get).map(this::quoteIfNeeded).toList();
                    writer.write(String.join(",", values));
                    writer.write("\n");
                }
            } else {
                // Heterogeneous list
                indent(indent);
                writer.write(key + "[" + list.size() + "]:\n");
                for (var item : list) {
                    indent(indent + 1);
                    writer.write("- ");
                    if (item instanceof Map<?, ?> obj) {
                        if (obj.size() == 1 && obj.values().stream().noneMatch(v -> v instanceof Map || v instanceof List)) {
                            var entry = obj.entrySet().iterator().next();
                            writer.write(formatKey(entry.getKey().toString()) + ": " + quoteIfNeeded(entry.getValue()) + "\n");
                        } else {
                            writer.write("\n");
                            writeObject(obj, indent + 2);
                        }
                    } else {
                        writer.write(quoteIfNeeded(item));
                        writer.write("\n");
                    }
                }
            }
        }

        private String quoteIfNeeded(Object value) {
            if (value == null) return "null";

            var s = value.toString().trim();

            // Numeric or boolean: no quotes
            if (s.matches("^-?\\d+(\\.\\d+)?$") || s.equals("true") || s.equals("false"))
                return s;

            // Must be quoted?
            if (s.isEmpty()
                || s.matches("^(null)$")
                || s.contains(":")
                || s.contains("\"")
                || s.contains(",")
                || s.contains("[")
                || s.contains("]")
                || s.contains("{")
                || s.contains("}")
                || s.startsWith("-")
                || s.startsWith(" ")
                || s.endsWith(" ")
            ) {
                return "\"" + escape(s) + "\"";
            }

            return s;
        }

        private String escape(String s) {
            return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
        }

        private String formatKey(String key) {
            if (key.matches("^[A-Za-z_][A-Za-z0-9_.]*$")) return key;
            return "\"" + escape(key) + "\"";
        }

        private void indent(int level) throws IOException {
            writer.write(INDENT_UNIT.repeat(level));
        }
    }
}
