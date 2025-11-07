package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    },
   metrics = {
       @Metric(
           name = "records",
           type = Counter.TYPE
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

        try (
            InputStream inputStream = runContext.storage().getFile(URI.create(String.valueOf(rFrom)));
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, rCharset),
                FileSerde.BUFFER_SIZE
            );
            BufferedWriter writer = new BufferedWriter(
                new FileWriter(tempFile, Charset.forName(rCharset)),
                FileSerde.BUFFER_SIZE
            )
        ) {
            ObjectMapper mapper = JacksonMapper.ofJson();

            // Use streaming parser to avoid loading entire file into memory
            JsonParser jsonParser = mapper.getFactory().createParser(reader);

            long[] count = {0};

            try {
                JsonToken token = jsonParser.nextToken();

                if (token == null) {
                    // Empty file - do nothing
                } else if (token == JsonToken.START_ARRAY) {
                    // JSON array - stream each element
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        JsonNode node = mapper.readTree(jsonParser);
                        writer.write(mapper.writeValueAsString(node));
                        writer.newLine();
                        count[0]++;
                    }
                } else if (token == JsonToken.START_OBJECT) {
                    // Could be a single object or JSONL format
                    // Read the first object
                    JsonNode firstNode = mapper.readTree(jsonParser);
                    writer.write(mapper.writeValueAsString(firstNode));
                    writer.newLine();
                    count[0]++;

                    // Try to read more objects (JSONL format)
                    JsonToken nextToken;
                    while ((nextToken = jsonParser.nextToken()) != null) {
                        if (nextToken == JsonToken.START_OBJECT) {
                            JsonNode node = mapper.readTree(jsonParser);
                            writer.write(mapper.writeValueAsString(node));
                            writer.newLine();
                            count[0]++;
                        }
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Invalid JSON format. Expected JSON array or object, but found: " + token
                    );
                }
            } catch (com.fasterxml.jackson.core.JsonParseException e) {
                throw new IllegalArgumentException("Invalid JSON format: " + e.getMessage(), e);
            } finally {
                jsonParser.close();
            }

            runContext.metric(Counter.of("records", count[0]));
        }

        URI outputUri = runContext.storage().putFile(tempFile);

        return Output.builder()
            .uri(outputUri)
            .build();
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
