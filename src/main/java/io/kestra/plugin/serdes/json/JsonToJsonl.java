package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Schema(
        title = "Convert a Json file into JsonL"
)
@Plugin(
        examples = {
                @Example(
                        title = "Convert a JSON array from an API to JSONL format for iteration.",
                        full = true,
                        code = """
                id: json_to_jsonl_example
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://api.restful-api.dev/objects"
                    contentType: application/json
                    method: GET

                  - id: json_to_jsonl
                    type: io.kestra.plugin.serdes.json.JsonToJsonl
                    from: "{{ outputs.download.uri }}"

                  - id: for_each_item
                    type: io.kestra.plugin.core.flow.ForEachItem
                    items: "{{ outputs.json_to_jsonl.uri }}"
                    batch:
                      rows: 1
                    namespace: company.team
                    flowId: process_item
                    wait: true
                    transmitFailed: true
                    inputs:
                      json: "{{ json(read(taskrun.items)) }}"
                """
                ),
                @Example(
                        title = "Convert JSON file with custom charset.",
                        code = """
                id: json_to_jsonl
                type: io.kestra.plugin.serdes.json.JsonToJsonl
                from: "{{ outputs.extract.uri }}"
                charset: ISO-8859-1
                """
                )
        }
)
public class JsonToJsonl extends Task implements RunnableTask<JsonToJsonl.Output> {

    @Builder.Default
    @Schema(
            title = "The name of a supported charset",
            description = "The character set to use for reading and writing the file."
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());
    @NotNull
    @Schema(
            title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {

        URI rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());

        var rCharset = runContext.render(charset).as(String.class).orElseThrow();

        File tempFile = runContext.workingDir().createTempFile(".jsonl").toFile();

        long count = 0;

        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                runContext.storage().getFile(URI.create(String.valueOf(rFrom))),
                                rCharset
                        )
                );
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(tempFile),
                                rCharset
                        )
                )
        ) {
            ObjectMapper mapper = new ObjectMapper();

            String firstLine = reader.readLine();
            if (firstLine == null || firstLine.trim().isEmpty()) {
                // Empty file
                runContext.metric(Counter.of("records", 0));
                URI outputUri = runContext.storage().putFile(tempFile);
                return Output.builder()
                        .uri(outputUri)
                        .build();
            }

            // Check if it's a JSON array or single object
            String trimmedFirst = firstLine.trim();

            if (trimmedFirst.startsWith("[")) {
                // If it's a JSON array - read the entire content
                StringBuilder jsonContent = new StringBuilder(firstLine);
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonContent.append(line);
                }

                // Parse an array
                JsonNode rootNode = mapper.readTree(jsonContent.toString());

                if (rootNode.isArray()) {
                    // Iterate through the array elements
                    Iterator<JsonNode> elements = rootNode.elements();
                    while (elements.hasNext()) {
                        JsonNode element = elements.next();
                        writer.write(mapper.writeValueAsString(element));
                        writer.newLine();
                        count++;
                    }
                } else {
                    // Single object wrapped in parsing
                    writer.write(mapper.writeValueAsString(rootNode));
                    writer.newLine();
                    count++;
                }
            } else if (trimmedFirst.startsWith("{")) {
                // Could be either a single JSON object or already JSONL format
                // Try to parse as a complete JSON object first
                try {
                    StringBuilder jsonContent = new StringBuilder(firstLine);
                    String line;

                    // Read until we have a complet JSON object
                    int braceCount = countBraces(firstLine);

                    if (braceCount == 0) {
                        // Complete object on first line - likely JSON format
                        writer.write(trimmedFirst);
                        writer.newLine();
                        count++;

                        // Process remaining lines as JSONL
                        while ((line = reader.readLine()) != null) {
                            String trimmedLine = line.trim();
                            if (!trimmedLine.isEmpty()) {
                                writer.write(trimmedLine);
                                writer.newLine();
                                count++;
                            }
                        }
                    } else {
                        // Multiline JSON object - read until complete
                        while (braceCount != 0 && (line = reader.readLine()) != null) {
                            jsonContent.append(line);
                            braceCount += countBraces(line);
                        }

                        // Parse and write the single object
                        JsonNode node = mapper.readTree(jsonContent.toString());
                        writer.write(mapper.writeValueAsString(node));
                        writer.newLine();
                        count++;

                        // Check if there are more objects
                        while ((line = reader.readLine()) != null) {
                            String trimmerLine = line.trim();
                            if (!trimmerLine.isEmpty()) {
                                JsonNode additionalNode = mapper.readTree(trimmerLine);
                                writer.write(mapper.writeValueAsString(additionalNode));
                                writer.newLine();
                                count++;
                            }
                        }
                    }
                } catch (Exception e) {

                    writer.write(trimmedFirst);
                    writer.newLine();
                    count++;

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String trimmedLine = line.trim();
                        if (!trimmedLine.isEmpty()) {
                            writer.write(trimmedLine);
                            writer.newLine();
                            count++;
                        }
                    }
                }
            } else {
                throw new IllegalArgumentException(
                        "Invalid JSON format. Expected JSON array starting with '[' or JSON object starting with '{', " +
                                "but found: " + trimmedFirst.substring(0, Math.min(50, trimmedFirst.length()))

                );
            }
        }

        URI outputUri = runContext.storage().putFile(tempFile);

        runContext.metric(Counter.of("records", count));

        return Output.builder()
                .uri(outputUri)
                .build();
    }

    /**
     * Count the net braces in a line (opening - closing)
     * Returns 0 when braces are balanced
     */
    private int countBraces(String line) {
        int count = 0;
        boolean inString = false;
        boolean escaped = false;

        for (char c : line.toCharArray()) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (!inString) {
                if (c == '{') {
                    count++;
                } else if (c == '}') {
                    count--;
                }
            }
        }
        return count;
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "URI of the generated JSONL file in Kestra's internal storage"
        )
        private final URI uri;
    }
}
