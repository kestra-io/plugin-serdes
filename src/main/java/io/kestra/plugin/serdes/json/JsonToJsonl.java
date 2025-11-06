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
import io.kestra.core.serializers.JacksonMapper;
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
                id: parent_json_processing
                namespace: company.team
                description: Parent flow that distributes work to subflows

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://api.restful-api.dev/objects"
                    contentType: application/json
                    method: GET
                    failOnEmptyResponse: true
                    timeout: PT15S

                  - id: json_to_jsonl
                    type: io.kestra.plugin.serdes.json.JsonToJsonl
                    from: "{{ outputs.download.uri }}"

                  - id: for_each_item
                    type: io.kestra.plugin.core.flow.ForEachItem
                    items: "{{ outputs.json_to_jsonl.uri }}"
                    batch:
                      rows: 1
                    namespace: company.team
                    flowId: child_process_item
                    wait: true
                    transmitFailed: true
                    inputs:
                      item_data: "{{ taskrun.items }}"
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
            ObjectMapper mapper = JacksonMapper.ofJson();

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
               count = processJsonArray(reader, writer, mapper, trimmedFirst);
            } else if (trimmedFirst.startsWith("{")) {
               count = processJsonLines(reader, writer, mapper, trimmedFirst);
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

    private long processJsonArray(BufferedReader reader, BufferedWriter writer,
                                  ObjectMapper mapper, String firstLine) throws IOException {
        long count = 0;
        StringBuilder objBuilder = new StringBuilder();
        int braceDepth = 0;
        boolean inString = false;

        // Start after '['
        String content = firstLine.substring(1);

        String allContent = content + readRemainingLines(reader);

        int i = 0;
        while (i < allContent.length()) {
            char c = allContent.charAt(i);

            if (c == '"' && (i == 0 || allContent.charAt(i-1) != '\\')) {
                inString = !inString;
            }

            if (!inString) {
                if (c == '{') {
                    braceDepth++;
                    objBuilder.append(c);
                } else if (c == '}') {
                    objBuilder.append(c);
                    braceDepth--;

                    if (braceDepth == 0 && !objBuilder.isEmpty()) {
                        // Complete object
                        String jsonObj = objBuilder.toString().trim();
                        if (!jsonObj.isEmpty()) {
                            JsonNode node = mapper.readTree(jsonObj);
                            writer.write(mapper.writeValueAsString(node));
                            writer.newLine();
                            count++;
                        }
                        objBuilder = new StringBuilder();
                    }
                } else if (c == ']' && braceDepth == 0) {
                    break;
                } else if (braceDepth > 0) {
                    objBuilder.append(c);
                }
            } else {
                objBuilder.append(c);
            }
            i++;
        }

        return count;
    }


    private String readRemainingLines(BufferedReader reader) throws IOException{
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }

    private long processJsonLines(BufferedReader reader, BufferedWriter writer, ObjectMapper mapper, String firstLine) throws IOException {
        long count = 0;

        if (!firstLine.isEmpty()) {
            JsonNode node = mapper.readTree(firstLine);
            writer.write(mapper.writeValueAsString(node));
            writer.newLine();
            count++;
        }

        String line;
        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            JsonNode node = mapper.readTree(trimmed);
            writer.write(mapper.writeValueAsString(node));
            writer.newLine();
            count++;
        }

        return count;
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
