package io.kestra.plugin.serdes.json;

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
import java.util.*;
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
        TOON is indentation-based, supports tabular arrays and minimal quoting.
        The converter automatically detects nested structures, lists, and tabular objects.
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
        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var tempFile = runContext.workingDir().createTempFile(".json").toFile();
        var renderedCharset = runContext.render(this.charset).as(String.class).orElseThrow();

        Object result;
        long count;
        try (
            var reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), Charset.forName(renderedCharset)), FileSerde.BUFFER_SIZE);
            var writer = new BufferedWriter(new FileWriter(tempFile, Charset.forName(renderedCharset)), FileSerde.BUFFER_SIZE)
        ) {
            var parser = new ToonParser(reader);
            result = parser.parse();
            count = parser.getCount();
            MAPPER.writeValue(writer, result);
        }

        runContext.metric(Counter.of("records", count));

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "URI of the resulting JSON file")
        private final URI uri;
    }

    private static class ToonParser {
        private final List<String> lines;
        private int index = 0;
        @Getter
        private long count = 0;

        private static final Pattern KEY_ARRAY_PATTERN =
            Pattern.compile("^(.*?)\\[(\\d+)](?:\\{([^}]*)})?$");

        ToonParser(BufferedReader reader) throws IOException {
            lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
        }

        public Object parse() {
            return parseBlock(0);
        }

        private Object parseBlock(int indent) {
            var obj = new LinkedHashMap<String, Object>();

            while (index < lines.size()) {
                var line = lines.get(index);

                var currentIndent = countIndent(line);
                if (currentIndent < indent) break;
                if (currentIndent > indent) {
                    index++;
                    continue;
                }

                if (line.isBlank()) {
                    index++;
                    continue;
                }

                var trimmed = line.stripLeading();
                index++;

                if (trimmed.startsWith("- ")) {
                    index--;
                    break;
                }

                var sepIndex = trimmed.indexOf(":");
                if (sepIndex == -1) continue;

                var keyPart = trimmed.substring(0, sepIndex).trim();
                var rest = trimmed.substring(sepIndex + 1).trim();

                var m = KEY_ARRAY_PATTERN.matcher(keyPart);
                if (m.matches()) {
                    var key = m.group(1).trim();
                    var size = Integer.parseInt(m.group(2));
                    var fields = m.group(3);

                    // Inline array
                    if (!rest.isEmpty()) {
                        var values = Arrays.stream(rest.split(","))
                            .map(String::trim)
                            .filter(v -> !v.isEmpty())
                            .map(this::parseScalar)
                            .toList();
                        obj.put(key, values);
                        count += values.size();
                        continue;
                    }

                    // Empty array
                    if (size == 0) {
                        obj.put(key, new ArrayList<>());
                        continue;
                    } else if (fields != null) {
                        // Tabular array
                        var table = new ArrayList<Map<String, Object>>();
                        for (int i = 0; i < size && index < lines.size(); i++) {
                            var row = lines.get(index).strip();
                            if (row.isEmpty()) break;
                            index++;
                            var cols = fields.split(",");
                            var vals = row.split(",", -1);
                            var rowMap = new LinkedHashMap<String, Object>();
                            for (int j = 0; j < cols.length && j < vals.length; j++) {
                                rowMap.put(cols[j].trim(), parseScalar(vals[j].trim()));
                            }
                            table.add(rowMap);
                            count++;
                        }
                        obj.put(key, table);
                        continue;
                    } else {
                        // Regular array
                        var arr = new ArrayList<>();
                        for (int i = 0; i < size && index < lines.size(); i++) {
                            var next = lines.get(index);
                            if (countIndent(next) <= currentIndent) break;
                            arr.add(parseListItem(currentIndent + 1));
                        }
                        obj.put(key, arr);
                        continue;
                    }
                }

                // Simple key: value or nested object
                if (rest.isEmpty()) {
                    var nested = parseBlock(indent + 1);
                    obj.put(keyPart, nested);
                } else {
                    obj.put(keyPart, parseScalar(rest));
                }
            }

            return obj;
        }

        private Object parseListItem(int indent) {
            var line = lines.get(index).stripLeading();
            if (!line.startsWith("-")) return null;
            index++;
            var rest = line.substring(1).trim();

            if (rest.isEmpty() && index < lines.size() && countIndent(lines.get(index)) > indent) {
                return parseBlock(indent + 1);
            } else if (rest.contains(":") && !rest.contains(",")) {
                var o = new LinkedHashMap<String, Object>();
                var kv = rest.split(":", 2);
                o.put(kv[0].trim(), parseScalar(kv[1].trim()));
                return o;
            } else {
                return parseScalar(rest);
            }
        }

        private int countIndent(String line) {
            var c = 0;
            while (c < line.length() && line.charAt(c) == ' ') c++;
            return c / 2;
        }

        private Object parseScalar(String s) {
            if (s == null || s.isEmpty()) return "";
            if (s.equals("null")) return null;
            if (s.equals("true")) return true;
            if (s.equals("false")) return false;
            if (s.startsWith("\"") && s.endsWith("\"")) {
                return s.substring(1, s.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\n", "\n")
                    .replace("\\r", "\r")
                    .replace("\\t", "\t")
                    .replace("\\\\", "\\");
            }
            if (s.matches("^-?\\d+$")) {
                try {
                    return Long.parseLong(s);
                } catch (NumberFormatException ignored) {}
            }
            if (s.matches("^-?\\d*\\.\\d+$")) {
                try {
                    return Double.parseDouble(s);
                } catch (NumberFormatException ignored) {}
            }
            return s;
        }
    }
}
