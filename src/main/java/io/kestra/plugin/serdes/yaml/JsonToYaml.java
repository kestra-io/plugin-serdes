package io.kestra.plugin.serdes.yaml;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
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
import java.util.List;

@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Schema(
    title = "Convert a JSON or JSONL file into YAML."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a JSON file to YAML",
            code = """
                id: json_to_yaml
                namespace: company.team

                tasks:
                  - id: write_json
                    type: io.kestra.plugin.core.storage.Write
                    extension: json
                    content: |
                      {
                        "name": "Apple",
                        "price": 1.2
                      }

                  - id: to_yaml
                    type: io.kestra.plugin.serdes.yaml.JsonToYaml
                    from: "{{ outputs.write_json.uri }}"
                """
        ),
        @Example(
            full = true,
            title = "Convert a JSONL file to YAML",
            code = """
                id: jsonl_to_yaml
                namespace: company.team

                tasks:
                  - id: write_jsonl
                    type: io.kestra.plugin.core.storage.Write
                    extension: jsonl
                    content: |
                      {"name":"Apple","price":1.2}
                      {"name":"Banana","price":0.9}

                  - id: to_yaml
                    type: io.kestra.plugin.serdes.yaml.JsonToYaml
                    jsonl: true
                    from: "{{ outputs.write_jsonl.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of JSON objects converted", type = Counter.TYPE),
    }
)
public class JsonToYaml extends Task implements RunnableTask<JsonToYaml.Output> {
    private static final ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();
    private static final ObjectMapper YAML_MAPPER = JacksonMapper.ofYaml().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    @NotNull
    @PluginProperty(internalStorageURI = true)
    @Schema(title = "Source file URI")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(
        title = "Input is JSONL (newline-delimited JSON)",
        description = "If true, each line is parsed as a separate JSON object and output as an element in a YAML list."
    )
    private Property<Boolean> jsonl = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());
        boolean isJsonl = runContext.render(jsonl).as(Boolean.class).orElse(false);
        String rCharset = runContext.render(charset).as(String.class).orElse(StandardCharsets.UTF_8.name());

        File tempFile = runContext.workingDir().createTempFile(".yaml").toFile();

        long count;

        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(runContext.storage().getFile(rFrom), rCharset),
                FileSerde.BUFFER_SIZE
            );
            Writer writer = new BufferedWriter(
                new FileWriter(tempFile, Charset.forName(rCharset)),
                FileSerde.BUFFER_SIZE
            )
        ) {
            if (isJsonl) {
                String line;
                count = 0;

                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    JsonNode node = JSON_MAPPER.readTree(line);

                    writer.write("---\n");
                    YAML_MAPPER.writeValue(writer, node);
                    writer.write("\n");

                    count++;
                }
            } else {
                JsonParser parser = JSON_MAPPER.getFactory().createParser(reader);
                JsonNode root = JSON_MAPPER.readTree(parser);

                YAML_MAPPER.writeValue(writer, root);
                count = 1;
            }
        }

        runContext.metric(Counter.of("records", count));

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the output file")
        private final URI uri;
    }
}
