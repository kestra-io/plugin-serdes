package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a TOON file into JSON.",
    description = """
        Parses a TOON (Token-Oriented Object Notation) document and converts it to JSON.
        Supports TOON 2.0 features emitted by JsonToToon:
        - indentation-based structure
        - primitive arrays (inline)
        - tabular arrays
        - list arrays
        - objects and arrays as list items
        - canonical primitives (true/false/null/numbers)
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a TOON file to JSON.",
            code = """
                id: toon_to_json
                namespace: company.team

                tasks:
                  - id: create_toon
                    type: io.kestra.plugin.core.storage.Write
                    extension: toon
                    content: |
                      products[2]{id,name,price}:
                        1,Apple,1.2
                        2,Banana,0.9
                      metadata:
                        category: fruits
                        country: France

                  - id: to_json
                    type: io.kestra.plugin.serdes.json.ToonToJson
                    from: "{{ outputs.create_toon.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records parsed", type = Counter.TYPE),
    }
)
public class ToonToJson extends Task implements RunnableTask<ToonToJson.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson().copy()
        .enable(SerializationFeature.INDENT_OUTPUT);

    @NotNull
    @Schema(title = "Source TOON file URI")
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
        var rCharset = runContext.render(this.charset).as(String.class).orElseThrow();
        var tempFile = runContext.workingDir().createTempFile(".json").toFile();

        long count;
        try (
            var reader = new BufferedReader(
                new InputStreamReader(runContext.storage().getFile(rFrom), Charset.forName(rCharset)),
                FileSerde.BUFFER_SIZE
            );
            var writer = new BufferedWriter(
                new FileWriter(tempFile, Charset.forName(rCharset)),
                FileSerde.BUFFER_SIZE
            )
        ) {
            var parser = new ToonParser(reader);
            Object result = parser.parse();
            count = parser.getCount();

            JsonNode node = toJsonNode(result);
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(writer, node);
        }

        runContext.metric(Counter.of("records", count));

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private static JsonNode toJsonNode(Object value) {
        var f = JsonNodeFactory.instance;

        switch (value) {
            case null -> {
                return NullNode.instance;
            }
            case Map<?, ?> map -> {
                var obj = f.objectNode();
                for (var e : map.entrySet()) {
                    obj.set(String.valueOf(e.getKey()), toJsonNode(e.getValue()));
                }
                return obj;
            }
            case Iterable<?> it -> {
                var arr = f.arrayNode();
                for (Object v : it) {
                    arr.add(toJsonNode(v));
                }
                return arr;
            }
            default -> {
            }
        }

        return MAPPER.valueToTree(value);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the resulting JSON file")
        private final URI uri;
    }

    /**
     * TOON 2.0 parser for the subset emitted by JsonToToon.
     * <p>
     * Supports:
     * - Objects with "key: value" and "key:" nested forms
     * - Array headers "key[N]:" and "key[N]{fields}:"
     * - Root array headers "[N]:" and "[N]{fields}:"
     * - Inline primitive arrays "key[N]: v1,v2"
     * - Tabular arrays "key[N]{a,b}:" + rows
     * - List arrays "key[N]:" + "- item"
     * - Objects as list items "- key: value", "- key:", "-" (empty object)
     * - Arrays as list items "- [M]: v1,v2" or "- [M]:"
     * - First-field arrays in list objects "- key[M]: ..."
     */
    private static class ToonParser {
        private static final int INDENT_SIZE = 2;

        private static final Pattern KEY_ARRAY_PATTERN =
            Pattern.compile("^(.*?)\\[(\\d+)](?:\\{([^}]*)})?$");

        private final List<Line> lines = new ArrayList<>();
        private int index = 0;

        @Getter
        private long count = 0;

        /**
         * @param text   line without leading indentation
         * @param indent indentation level (0,1,2,...) in units of INDENT_SIZE
         */
        private record Line(String text, int indent) {
        }

        ToonParser(BufferedReader reader) throws IOException {
            String raw;
            while ((raw = reader.readLine()) != null) {
                if (raw.trim().isEmpty()) {
                    continue;
                }
                int spaces = 0;
                while (spaces < raw.length() && raw.charAt(spaces) == ' ') {
                    spaces++;
                }
                if (spaces % INDENT_SIZE != 0) {
                    throw new IllegalArgumentException(
                        "Invalid indentation: must be a multiple of " + INDENT_SIZE + " spaces"
                    );
                }
                int indent = spaces / INDENT_SIZE;
                String text = raw.substring(spaces);
                lines.add(new Line(text, indent));
            }
        }

        public Object parse() {
            if (lines.isEmpty()) {
                // Empty document -> empty object
                return new LinkedHashMap<String, Object>();
            }

            Line first = lines.getFirst();

            // Root array header form: [N]: or [N]{fields}:
            if (first.indent == 0 && isRootArrayHeader(first.text)) {
                return parseRootArray();
            }

            // Root primitive form: single primitive line without colon or header
            if (!first.text.contains(":") && !first.text.startsWith("[") && lines.size() == 1) {
                index = 1;
                return parseScalar(first.text.trim());
            }

            // Default: root object
            index = 0;
            return parseObject(0);
        }

        private boolean isRootArrayHeader(String text) {
            int colonIdx = text.indexOf(':');
            if (colonIdx < 0) {
                return false;
            }
            String header = text.substring(0, colonIdx).trim();
            Matcher m = KEY_ARRAY_PATTERN.matcher(header);
            if (!m.matches()) {
                return false;
            }
            String key = m.group(1).trim();
            return key.isEmpty();
        }

        private Object parseRootArray() {
            Line first = lines.getFirst();
            int colonIdx = first.text.indexOf(':');
            String header = first.text.substring(0, colonIdx).trim();
            String rest = first.text.substring(colonIdx + 1).trim();

            Matcher m = KEY_ARRAY_PATTERN.matcher(header);
            if (!m.matches()) {
                throw new IllegalArgumentException("Invalid root array header: " + header);
            }

            String key = m.group(1).trim(); // must be empty for root array
            if (!key.isEmpty()) {
                throw new IllegalArgumentException("Root array header must not have a key: " + header);
            }

            int size = Integer.parseInt(m.group(2));
            String fields = m.group(3);

            index = 1;
            return parseArrayFromHeader(size, fields, rest, 0);
        }

        private Map<String, Object> parseObject(int indentLevel) {
            Map<String, Object> obj = new LinkedHashMap<>();

            while (index < lines.size()) {
                Line line = lines.get(index);

                if (line.indent < indentLevel) {
                    break;
                }
                if (line.indent > indentLevel) {
                    // This line belongs to a nested structure already consumed.
                    break;
                }

                String text = line.text;

                // List items at this depth belong to an array, not to this object
                if (text.startsWith("- ")) {
                    break;
                }

                index++;
                int colonIdx = text.indexOf(':');
                if (colonIdx < 0) {
                    continue;
                }

                String keyPart = text.substring(0, colonIdx).trim();
                String rest = text.substring(colonIdx + 1).trim();

                Matcher m = KEY_ARRAY_PATTERN.matcher(keyPart);
                if (m.matches()) {
                    // Array field: key[N]: ..., key[N]{fields}:
                    String key = m.group(1).trim();
                    int size = Integer.parseInt(m.group(2));
                    String fields = m.group(3);

                    Object arrayValue = parseArrayFromHeader(size, fields, rest, indentLevel);
                    obj.put(key, arrayValue);
                    continue;
                }

                // Nested object: "key:"
                if (rest.isEmpty()) {
                    Object nested = parseObject(indentLevel + 1);
                    obj.put(keyPart, nested);
                } else {
                    // Primitive: "key: value"
                    obj.put(keyPart, parseScalar(rest));
                }
            }

            return obj;
        }

        /**
         * Parse an array declared by a header:
         * - inline primitive arrays: key[N]: v1,v2
         * - tabular arrays: key[N]{f1,f2}:
         * - list arrays: key[N]:
         */
        private Object parseArrayFromHeader(int size, String fields, String rest, int headerIndent) {
            // Inline primitive array: key[N]: v1,v2,...
            if (!rest.isEmpty()) {
                List<Object> values = parseDelimitedValues(rest);
                count += values.size();
                return values;
            }

            // Empty array: key[0]:
            if (size == 0) {
                return new ArrayList<>();
            }

            // Tabular array: key[N]{fields}:
            if (fields != null) {
                String[] cols = splitFields(fields);
                List<Map<String, Object>> table = new ArrayList<>();

                int rowsRead = 0;
                while (rowsRead < size && index < lines.size()) {
                    Line rowLine = lines.get(index);
                    if (rowLine.indent <= headerIndent) {
                        break;
                    }
                    // Expect rows at headerIndent+1
                    if (rowLine.indent != headerIndent + 1) {
                        break;
                    }

                    String rowText = rowLine.text.trim();
                    index++;

                    List<String> cellTokens = splitByCommaRespectingQuotes(rowText);
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int j = 0; j < cols.length && j < cellTokens.size(); j++) {
                        String colName = cols[j].trim();
                        String raw = cellTokens.get(j).trim();
                        row.put(colName, parseScalar(raw));
                    }
                    table.add(row);
                    rowsRead++;
                    count++;
                }

                return table;
            }

            // List array: key[N]:
            List<Object> arr = new ArrayList<>();
            int items = 0;

            while (items < size && index < lines.size()) {
                Line itemLine = lines.get(index);
                if (itemLine.indent <= headerIndent) {
                    break;
                }
                Object item = parseListItem(headerIndent + 1);
                arr.add(item);
                items++;
                count++;
            }

            return arr;
        }

        /**
         * Parse a list item "- ..." at the given indentation level.
         */
        private Object parseListItem(int indentLevel) {
            if (index >= lines.size()) {
                return null;
            }

            Line line = lines.get(index);
            if (line.indent != indentLevel || !line.text.startsWith("-")) {
                return null;
            }

            String text = line.text.substring(1).trim(); // after '-'
            index++;

            // "-" alone: empty object
            if (text.isEmpty()) {
                // If there are deeper lines, they belong to a nested object, but
                // our encoder only uses "-" for empty objects, so we return {}.
                return new LinkedHashMap<String, Object>();
            }

            int colonIdx = text.indexOf(':');

            // No colon: either "- value" (primitive) or "- [N]{...}: ..." (but header always has colon)
            if (colonIdx < 0) {
                // Might be array header without colon but our encoder always adds colon.
                // So here it is a primitive list item.
                return parseScalar(text);
            }

            String beforeColon = text.substring(0, colonIdx).trim();
            String rest = text.substring(colonIdx + 1).trim();

            Matcher m = KEY_ARRAY_PATTERN.matcher(beforeColon);
            if (m.matches()) {
                // "- key[N]...:" or "- [N]...:"
                String key = m.group(1).trim();
                int size = Integer.parseInt(m.group(2));
                String fields = m.group(3);

                // "- [N]: ..." => array value as list item
                if (key.isEmpty()) {
                    Object arrayValue = parseArrayFromHeader(size, fields, rest, indentLevel);
                    return arrayValue;
                }

                // "- key[N]: ..." => first field is an array inside an object item
                Map<String, Object> obj = new LinkedHashMap<>();
                Object arrayValue = parseArrayFromHeader(size, fields, rest, indentLevel);
                obj.put(key, arrayValue);

                // Additional fields of the same object item at depth +1
                Map<String, Object> extra = parseObject(indentLevel + 1);
                obj.putAll(extra);

                return obj;
            }

            // "- key:" with nested object
            Map<String, Object> obj = new LinkedHashMap<>();
            if (rest.isEmpty()) {
                Object nested = parseObject(indentLevel + 1);
                obj.put(beforeColon, nested);
            } else {
                // "- key: value" primitive + optional extra fields
                obj.put(beforeColon, parseScalar(rest));

                Map<String, Object> extra = parseObject(indentLevel + 1);
                obj.putAll(extra);
            }
            return obj;
        }

        private String[] splitFields(String fields) {
            // Fields are keys, possibly quoted; our encoder never quotes field names
            return fields.split(",", -1);
        }

        /**
         * Split a comma-separated line into tokens, honoring double quotes.
         */
        private List<String> splitByCommaRespectingQuotes(String line) {
            List<String> result = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);

                if (c == '"') {
                    inQuotes = !inQuotes;
                    current.append(c);
                } else if (c == ',' && !inQuotes) {
                    result.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }

            result.add(current.toString());
            return result;
        }

        private List<Object> parseDelimitedValues(String line) {
            List<String> tokens = splitByCommaRespectingQuotes(line);
            List<Object> values = new ArrayList<>(tokens.size());
            for (String token : tokens) {
                values.add(parseScalar(token.trim()));
            }
            return values;
        }

        private Object parseScalar(String s) {
            if (s == null) {
                return null;
            }
            String trimmed = s.trim();
            switch (trimmed) {
                case "" -> {
                    return "";
                }

                // null literal
                case "null" -> {
                    return null;
                }

                // boolean literal
                case "true" -> {
                    return Boolean.TRUE;
                }
                case "false" -> {
                    return Boolean.FALSE;
                }
            }

            // quoted string
            if (trimmed.length() >= 2 &&
                trimmed.charAt(0) == '"' &&
                trimmed.charAt(trimmed.length() - 1) == '"') {
                return unescape(trimmed.substring(1, trimmed.length() - 1));
            }

            // number canonical form (integer or decimal, no exponent)
            if (trimmed.matches("^-?\\d+(?:\\.\\d+)?$")) {
                if (trimmed.contains(".")) {
                    // decimal => BigDecimal
                    try {
                        return new BigDecimal(trimmed);
                    } catch (NumberFormatException ignored) {
                        return trimmed;
                    }
                } else {
                    // integer => Long
                    try {
                        return Long.parseLong(trimmed);
                    } catch (NumberFormatException ignored) {
                        return new BigDecimal(trimmed); // fallback big integer
                    }
                }
            }

            // fallback: string
            return trimmed;
        }

        private String unescape(String s) {
            StringBuilder out = new StringBuilder(s.length());
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '\\' && i + 1 < s.length()) {
                    char n = s.charAt(i + 1);
                    switch (n) {
                        case '\\' -> {
                            out.append('\\');
                            i++;
                        }
                        case '"' -> {
                            out.append('"');
                            i++;
                        }
                        case 'n' -> {
                            out.append('\n');
                            i++;
                        }
                        case 'r' -> {
                            out.append('\r');
                            i++;
                        }
                        case 't' -> {
                            out.append('\t');
                            i++;
                        }
                        default -> out.append(n);
                    }
                } else {
                    out.append(c);
                }
            }
            return out.toString();
        }
    }
}
