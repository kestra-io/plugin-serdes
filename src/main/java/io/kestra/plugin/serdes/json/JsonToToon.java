package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.LinkedHashSet;
import java.util.Set;
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
            title = "Convert a JSON file to TOON format and summarize it with AI.",
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

                  - id: chat_completion
                    type: io.kestra.plugin.ai.completion.ChatCompletion
                    provider:
                      type: io.kestra.plugin.ai.provider.GoogleGemini
                      apiKey: "{{ kv('GOOGLE_API_KEY') }}"
                      modelName: gemini-2.5-flash
                    messages:
                      - type: SYSTEM
                        content: You are an assistant that summarizes data.
                      - type: USER
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
        var rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());
        var rCharset = runContext.render(charset).as(String.class).orElse(StandardCharsets.UTF_8.name());
        var tempFile = runContext.workingDir().createTempFile(".toon").toFile();

        var mapper = JacksonMapper.ofJson();
        var factory = mapper.getFactory();
        long count = 0;

        try (
            var inputStream = runContext.storage().getFile(rFrom);
            var reader = new BufferedReader(
                new InputStreamReader(inputStream, rCharset),
                FileSerde.BUFFER_SIZE
            );
            var writer = new BufferedWriter(
                new FileWriter(tempFile, Charset.forName(rCharset)),
                FileSerde.BUFFER_SIZE
            );
            var parser = factory.createParser(reader)
        ) {
            var token = parser.nextToken();
            var encoder = new ToonEncoder(writer);

            if (token == JsonToken.START_ARRAY) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    JsonNode node = mapper.readTree(parser);
                    encoder.writeNode(node);
                    count++;
                }
            } else if (token == JsonToken.START_OBJECT) {
                JsonNode node = mapper.readTree(parser);
                encoder.writeNode(node);
                count++;
            } else {
                throw new IllegalArgumentException(
                    "Invalid JSON: expected an array or object at root, found " + token
                );
            }

            writer.flush();
        }

        runContext.metric(Counter.of("records", count));

        URI outputUri = runContext.storage().putFile(tempFile);
        return Output.builder().uri(outputUri).build();
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

        ToonEncoder(Writer writer) {
            this.writer = writer;
        }

        void writeNode(JsonNode node) throws IOException {
            if (node.isObject()) {
                writeObject(node, 0);
            } else if (node.isArray()) {
                writeArray(node, 0, "root");
            } else {
                indent(0);
                writer.write(quoteIfNeeded(node.asText()));
                writer.write("\n");
            }
        }

        private void writeObject(JsonNode node, int indent) throws IOException {
            for (var entry : node.properties()) {
                var key = formatKey(entry.getKey());
                var value = entry.getValue();

                if (value.isObject()) {
                    indent(indent);
                    writer.write(key + ":\n");
                    writeObject(value, indent + 1);
                } else if (value.isArray()) {
                    writeArray(value, indent, key);
                } else {
                    indent(indent);
                    writer.write(key + ": " + quoteIfNeeded(value.asText()) + "\n");
                }
            }
        }

        private void writeArray(JsonNode array, int indent, String key) throws IOException {
            if (!array.isArray()) {
                indent(indent);
                writer.write(key + ": " + quoteIfNeeded(array.asText()) + "\n");
                return;
            }

            if (array.isEmpty()) {
                indent(indent);
                writer.write(key + "[0]:\n");
                return;
            }

            boolean allPrimitive = true;
            boolean allObjects = true;

            for (JsonNode item : array) {
                if (item.isObject() || item.isArray()) allPrimitive = false;
                if (!item.isObject()) allObjects = false;
            }

            // Inline primitive arrays
            if (allPrimitive) {
                indent(indent);
                writer.write(key + "[" + array.size() + "]: ");
                for (int i = 0; i < array.size(); i++) {
                    if (i > 0) writer.write(",");
                    writer.write(quoteIfNeeded(array.get(i).asText()));
                }
                writer.write("\n");
                return;
            }

            // Uniform object arrays (tabular)
            if (allObjects && !array.isEmpty()) {
                Set<String> fields = new LinkedHashSet<>();
                array.get(0).properties().forEach(entry -> fields.add(entry.getKey()));

                boolean uniform = true;
                for (JsonNode item : array) {
                    Set<String> keys = new LinkedHashSet<>();
                    item.properties().forEach(entry -> keys.add(entry.getKey()));
                    if (!keys.equals(fields)) {
                        uniform = false;
                        break;
                    }
                }

                if (uniform) {
                    indent(indent);
                    writer.write(key + "[" + array.size() + "]{" + String.join(",", fields) + "}:\n");
                    for (JsonNode row : array) {
                        indent(indent + 1);
                        boolean first = true;
                        for (String field : fields) {
                            if (!first) writer.write(",");
                            JsonNode cell = row.get(field);
                            writer.write(quoteIfNeeded(cell != null ? cell.asText() : "null"));
                            first = false;
                        }
                        writer.write("\n");
                    }
                    return;
                }
            }

            // Heterogeneous or mixed arrays
            indent(indent);
            writer.write(key + "[" + array.size() + "]:\n");
            for (JsonNode item : array) {
                indent(indent + 1);
                writer.write("- ");
                if (item.isObject()) {
                    if (item.size() == 1 && item.properties().iterator().next().getValue().isValueNode()) {
                        var entry = item.properties().iterator().next();
                        writer.write(formatKey(entry.getKey()) + ": " + quoteIfNeeded(entry.getValue().asText()) + "\n");
                    } else {
                        writer.write("\n");
                        writeObject(item, indent + 2);
                    }
                } else {
                    writer.write(quoteIfNeeded(item.asText()) + "\n");
                }
            }
        }

        private void indent(int level) throws IOException {
            writer.write(INDENT_UNIT.repeat(level));
        }

        private String quoteIfNeeded(String value) {
            if (value == null) return "null";
            String s = value.trim();

            // Null literal must stay unquoted
            if (s.equals("null")) {
                return "null";
            }

            // Numeric or boolean values: no quotes
            if (s.matches("^-?\\d+(\\.\\d+)?$") || s.equals("true") || s.equals("false")) {
                return s;
            }

            // Must be quoted if it contains structural/special characters
            if (s.isEmpty()
                || s.contains(":")
                || s.contains("\"")
                || s.contains(",")
                || s.contains("{")
                || s.contains("}")
                || s.contains("[")
                || s.contains("]")
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
    }
}
