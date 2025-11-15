package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
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
import java.math.BigDecimal;
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
        See https://github.com/toon-format/spec for the full specification (2.0).
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

    // Document delimiter is always comma in this implementation (valid TOON 2.0).
    private static final char DOCUMENT_DELIMITER = ',';

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

        long count;

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
            JsonParser parser = OBJECT_MAPPER.getFactory().createParser(reader)
        ) {
            // Read full JSON root node (object, array, or primitive).
            JsonNode root = OBJECT_MAPPER.readTree(parser);
            if (root == null) {
                throw new IllegalArgumentException("Invalid JSON: empty input");
            }

            ToonEncoder encoder = new ToonEncoder(writer);

            if (root.isArray()) {
                encoder.writeRootArray(root);
                count = root.size();
            } else if (root.isObject()) {
                encoder.writeRootObject(root);
                count = 1;
            } else {
                // Primitive root (number, boolean, string, null)
                encoder.writeRootPrimitive(root);
                count = 1;
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

    /**
     * TOON 2.0 compliant encoder for a JSON data model.
     * <p>
     * - Root arrays use headers: [N]: ... or [N]{fields}: ...
     * - Root objects use regular key: value lines.
     * - Root primitives use a single primitive line.
     * - Numbers are emitted in canonical decimal form without exponent.
     * - Strings follow TOON 2.0 quoting rules.
     * - Arrays of uniform objects are rendered in tabular form when applicable.
     * - Mixed arrays are rendered as list arrays with "- " items.
     * - No trailing newline is written at the end of the document.
     */
    private static class ToonEncoder {
        private static final int INDENT_SIZE = 2;
        private static final String INDENT_UNIT = " ".repeat(INDENT_SIZE);

        private final Writer writer;
        // Tracks whether we are writing the very first line to avoid trailing newline at EOF.
        private boolean firstLine = true;

        ToonEncoder(Writer writer) {
            this.writer = writer;
        }

        /* ---------- Root entry points ---------- */

        void writeRootObject(JsonNode node) throws IOException {
            if (node.isEmpty()) {
                // Empty root object => empty document (no lines).
                return;
            }
            writeObject(node, 0);
        }

        void writeRootArray(JsonNode array) throws IOException {
            writeArray(array, 0, null);
        }

        void writeRootPrimitive(JsonNode node) throws IOException {
            String value = formatPrimitive(node);
            writeLine(0, value);
        }

        /* ---------- Core writing helpers ---------- */

        private void writeObject(JsonNode node, int indent) throws IOException {
            // Object fields are emitted in encounter order.
            for (var entry : node.properties()) {
                String key = formatKey(entry.getKey());
                JsonNode value = entry.getValue();

                if (value.isObject()) {
                    // Nested or empty object: "key:" on its own line.
                    writeLine(indent, key + ":");
                    if (!value.isEmpty()) {
                        writeObject(value, indent + 1);
                    }
                } else if (value.isArray()) {
                    // Array field: render as array header + content.
                    writeArray(value, indent, key);
                } else {
                    // Primitive field: "key: value"
                    String v = formatPrimitive(value);
                    writeLine(indent, key + ": " + v);
                }
            }
        }

        /**
         * Encode an array according to TOON 2.0 rules.
         *
         * @param array  the array node
         * @param indent indentation level
         * @param key    object key or null for root arrays
         */
        private void writeArray(JsonNode array, int indent, String key) throws IOException {
            int size = array.size();

            // Empty array: key[0]: or [0]:
            if (size == 0) {
                String header = headerPrefix(key) + "[" + size + "]:";
                writeLine(indent, header);
                return;
            }

            boolean allPrimitive = true;
            boolean allObjects = true;

            for (JsonNode item : array) {
                if (item.isObject() || item.isArray()) {
                    allPrimitive = false;
                }
                if (!item.isObject()) {
                    allObjects = false;
                }
            }

            // Inline primitive arrays (9.1)
            if (allPrimitive) {
                writePrimitiveArray(array, indent, key);
                return;
            }

            // Uniform object arrays (tabular form, 9.3)
            if (allObjects && isUniformPrimitiveObjectArray(array)) {
                writeTabularArray(array, indent, key);
                return;
            }

            // Mixed / non-uniform arrays (9.4)
            writeListArray(array, indent, key);
        }

        private void writePrimitiveArray(JsonNode array, int indent, String key) throws IOException {
            int size = array.size();
            StringBuilder line = new StringBuilder();
            line.append(headerPrefix(key))
                .append("[")
                .append(size)
                .append("]:");

            if (size > 0) {
                line.append(' ');
                for (int i = 0; i < size; i++) {
                    if (i > 0) {
                        line.append(DOCUMENT_DELIMITER);
                    }
                    line.append(formatPrimitive(array.get(i)));
                }
            }

            writeLine(indent, line.toString());
        }

        private boolean isUniformPrimitiveObjectArray(JsonNode array) {
            if (array.isEmpty()) {
                return false;
            }

            Set<String> fields = new LinkedHashSet<>();
            array.get(0).properties().forEach(entry -> fields.add(entry.getKey()));

            if (fields.isEmpty()) {
                return false;
            }

            for (JsonNode item : array) {
                Set<String> keys = new LinkedHashSet<>();
                item.properties().forEach(entry -> keys.add(entry.getKey()));
                if (!keys.equals(fields)) {
                    return false;
                }
                // All values must be primitives
                for (var entry : item.properties()) {
                    if (entry.getValue().isObject() || entry.getValue().isArray()) {
                        return false;
                    }
                }
            }

            return true;
        }

        private void writeTabularArray(JsonNode array, int indent, String key) throws IOException {
            int size = array.size();

            // Collect ordered field names from first object
            Set<String> fields = new LinkedHashSet<>();
            array.get(0).properties().forEach(entry -> fields.add(entry.getKey()));

            StringBuilder header = new StringBuilder();
            header.append(headerPrefix(key))
                .append("[")
                .append(size)
                .append("]{");

            boolean first = true;
            for (String field : fields) {
                if (!first) {
                    header.append(DOCUMENT_DELIMITER);
                }
                header.append(formatKey(field));
                first = false;
            }
            header.append("}:");

            writeLine(indent, header.toString());

            // Rows at depth +1
            for (JsonNode row : array) {
                StringBuilder rowLine = new StringBuilder();
                boolean firstCell = true;
                for (String field : fields) {
                    if (!firstCell) {
                        rowLine.append(DOCUMENT_DELIMITER);
                    }
                    JsonNode cell = row.get(field);
                    rowLine.append(formatPrimitive(cell == null ? nullNode() : cell));
                    firstCell = false;
                }
                writeLine(indent + 1, rowLine.toString());
            }
        }

        private void writeListArray(JsonNode array, int indent, String key) throws IOException {
            int size = array.size();

            // Header: key[N]: or [N]:
            String header = headerPrefix(key) + "[" + size + "]:";
            writeLine(indent, header);

            // Each element rendered as a list item (9.4, 10)
            for (JsonNode item : array) {
                if (item.isObject()) {
                    writeListObjectItem(item, indent);
                } else if (item.isArray()) {
                    writeListArrayItem(item, indent);
                } else {
                    // Primitive item: "- value"
                    String v = formatPrimitive(item);
                    writeLine(indent + 1, "- " + v);
                }
            }
        }

        /**
         * Encode an object as a list item ("- ...") according to §10.
         */
        private void writeListObjectItem(JsonNode obj, int indent) throws IOException {
            var fieldsIter = obj.properties().iterator();

            if (!fieldsIter.hasNext()) {
                // Empty object list item: a single "-" at list-item depth.
                writeLine(indent + 1, "-");
                return;
            }

            var first = fieldsIter.next();
            String firstKey = formatKey(first.getKey());
            JsonNode firstValue = first.getValue();

            if (firstValue.isObject()) {
                // Nested object as first field: "- key:" then nested fields at depth +2.
                writeLine(indent + 1, "- " + firstKey + ":");
                if (!firstValue.isEmpty()) {
                    writeObject(firstValue, indent + 2);
                }
            } else if (firstValue.isArray()) {
                // First field is an array: "- key[N]: ..." or "- key[N]:\n  - ..."
                writeListArrayFirstField(firstKey, firstValue, indent + 1);
            } else {
                // Primitive first field: "- key: value"
                String v = formatPrimitive(firstValue);
                writeLine(indent + 1, "- " + firstKey + ": " + v);
            }

            // Remaining fields of the same object at depth +2
            while (fieldsIter.hasNext()) {
                var entry = fieldsIter.next();
                String key = formatKey(entry.getKey());
                JsonNode value = entry.getValue();

                if (value.isObject()) {
                    writeLine(indent + 2, key + ":");
                    if (!value.isEmpty()) {
                        writeObject(value, indent + 3);
                    }
                } else if (value.isArray()) {
                    writeFieldArray(key, value, indent + 2);
                } else {
                    String v = formatPrimitive(value);
                    writeLine(indent + 2, key + ": " + v);
                }
            }
        }

        /**
         * Encode an array field that is the first field on a list-item hyphen line.
         */
        private void writeListArrayFirstField(String key, JsonNode array, int hyphenIndent) throws IOException {
            int size = array.size();

            boolean allPrimitive = true;
            for (JsonNode item : array) {
                if (item.isObject() || item.isArray()) {
                    allPrimitive = false;
                    break;
                }
            }

            if (allPrimitive) {
                // "- key[N]: v1,v2,..."
                StringBuilder line = new StringBuilder();
                line.append("- ")
                    .append(key)
                    .append("[")
                    .append(size)
                    .append("]:");

                if (size > 0) {
                    line.append(' ');
                    for (int i = 0; i < size; i++) {
                        if (i > 0) {
                            line.append(DOCUMENT_DELIMITER);
                        }
                        line.append(formatPrimitive(array.get(i)));
                    }
                }

                writeLine(hyphenIndent, line.toString());
            } else {
                // "- key[N]:", then nested list items at depth +1 relative to header
                String header = "- " + key + "[" + size + "]:";
                writeLine(hyphenIndent, header);
                writeListArray(array, hyphenIndent + 1, null);
            }
        }

        /**
         * Encode an array field inside an object (non-list context).
         */
        private void writeFieldArray(String key, JsonNode array, int indent) throws IOException {
            writeArray(array, indent, key);
        }

        /**
         * Encode an array as a list item content (for array elements inside a list array).
         */
        private void writeListArrayItem(JsonNode arrayNode, int indent) throws IOException {
            int size = arrayNode.size();

            boolean allPrimitive = true;
            for (JsonNode item : arrayNode) {
                if (item.isObject() || item.isArray()) {
                    allPrimitive = false;
                    break;
                }
            }

            if (allPrimitive) {
                // Arrays of primitives as items: "- [M]: v1,v2,..."
                StringBuilder line = new StringBuilder();
                line.append("- [")
                    .append(size)
                    .append("]:");

                if (size > 0) {
                    line.append(' ');
                    for (int i = 0; i < size; i++) {
                        if (i > 0) {
                            line.append(DOCUMENT_DELIMITER);
                        }
                        line.append(formatPrimitive(arrayNode.get(i)));
                    }
                }

                writeLine(indent + 1, line.toString());
            } else {
                // Complex nested array: "- [M]:", then nested list items at depth +2
                String header = "- [" + size + "]:";
                writeLine(indent + 1, header);

                // Render nested array items as list items under this header
                for (JsonNode item : arrayNode) {
                    if (item.isObject()) {
                        writeListObjectItem(item, indent + 1);
                    } else if (item.isArray()) {
                        writeListArrayItem(item, indent + 1);
                    } else {
                        String v = formatPrimitive(item);
                        writeLine(indent + 2, "- " + v);
                    }
                }
            }
        }

        /* ---------- Formatting helpers ---------- */

        private String headerPrefix(String key) {
            return key == null ? "" : key;
        }

        private JsonNode nullNode() {
            return OBJECT_MAPPER.nullNode();
        }

        /**
         * Format a JSON value as a TOON primitive token according to §2, §4, §7.
         */
        private String formatPrimitive(JsonNode node) {
            if (node == null || node.isNull()) {
                return "null";
            }

            if (node.isNumber()) {
                return formatNumber(node);
            }

            if (node.isBoolean()) {
                return node.booleanValue() ? "true" : "false";
            }

            // Strings (including original JSON strings like "true", "1e6", etc.)
            String raw = node.textValue();
            return quoteStringIfNeeded(raw);
        }

        /**
         * Canonical number formatting:
         * - no exponent
         * - no unnecessary trailing zeros
         * - -0 normalized to 0
         */
        private String formatNumber(JsonNode node) {
            BigDecimal dec = node.decimalValue();

            if (dec.compareTo(BigDecimal.ZERO) == 0) {
                return "0";
            }

            String s = dec.stripTrailingZeros().toPlainString();

            // Ensure "-0" variants are normalized to "0"
            if (s.equals("-0")) {
                return "0";
            }

            return s;
        }

        /**
         * Apply TOON 2.0 quoting rules to a string value.
         */
        private String quoteStringIfNeeded(String value) {
            if (value == null) {
                return "null";
            }

            // Empty string
            if (value.isEmpty()) {
                return "\"" + escape(value) + "\"";
            }

            // Leading or trailing whitespace
            if (!value.equals(value.trim())) {
                return "\"" + escape(value) + "\"";
            }

            // Reserved literals: true, false, null (as strings, not booleans/null)
            if (value.equals("true") || value.equals("false") || value.equals("null")) {
                return "\"" + escape(value) + "\"";
            }

            // Numeric-like tokens (including exponent) or leading-zero decimals
            if (value.matches("^-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?$") || value.matches("^0\\d+$")) {
                return "\"" + escape(value) + "\"";
            }

            // Structural / special characters
            if (value.indexOf(':') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\\') >= 0) {
                return "\"" + escape(value) + "\"";
            }

            if (value.indexOf('[') >= 0 || value.indexOf(']') >= 0 || value.indexOf('{') >= 0 || value.indexOf('}') >= 0) {
                return "\"" + escape(value) + "\"";
            }

            // Control characters
            if (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0 || value.indexOf('\t') >= 0) {
                return "\"" + escape(value) + "\"";
            }

            // Relevant delimiter (document delimiter or active delimiter)
            if (value.indexOf(JsonToToon.DOCUMENT_DELIMITER) >= 0) {
                return "\"" + escape(value) + "\"";
            }

            // Strings equal "-" or starting with "-"
            if (value.equals("-") || value.startsWith("-")) {
                return "\"" + escape(value) + "\"";
            }

            // Safe to emit without quotes
            return value;
        }

        private String escape(String s) {
            return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
        }

        /**
         * Keys and field names: unquoted only if they match ^[A-Za-z_][A-Za-z0-9_.]*$
         */
        private String formatKey(String key) {
            if (key.matches("^[A-Za-z_][A-Za-z0-9_.]*$")) {
                return key;
            }
            return "\"" + escape(key) + "\"";
        }

        /* ---------- Line / indentation helpers ---------- */

        /**
         * Write a full logical line with indentation and ensure:
         * - LF line ending between lines
         * - no trailing newline at end of document
         */
        private void writeLine(int indentLevel, String content) throws IOException {
            if (!firstLine) {
                writer.write("\n");
            }
            indent(indentLevel);
            writer.write(content);
            firstLine = false;
        }

        private void indent(int level) throws IOException {
            if (level <= 0) {
                return;
            }
            writer.write(INDENT_UNIT.repeat(level));
        }
    }
}
