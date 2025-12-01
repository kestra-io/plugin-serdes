package io.kestra.plugin.serdes.yaml;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.*;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Schema(
    title = "Convert a YAML file into JSON or JSONL.",
    description = """
        If `jsonl = true`, each YAML document (`---`) becomes one JSON line.
        If `jsonl = false`, output becomes:
          - a JSON array when there are multiple YAML documents
          - a single JSON object when there is exactly one document
          - a JSON array when the YAML is a list
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Convert a YAML file into JSONL",
            full = true,
            code = """
                id: yaml_to_json
                namespace: company.team

                tasks:
                  - id: write_yaml
                    type: io.kestra.plugin.core.storage.Write
                    extension: yaml
                    content: |
                      ---
                      name: Apple
                      price: 1.2
                      ---
                      name: Banana
                      price: 0.9

                  - id: to_jsonl
                    type: io.kestra.plugin.serdes.yaml.YamlToJson
                    from: "{{ outputs.write_yaml.uri }}"
                    jsonl: true
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of YAML documents converted", type = Counter.TYPE),
    }
)
public class YamlToJson extends Task implements RunnableTask<YamlToJson.Output> {
    private static final ObjectMapper YAML_MAPPER = JacksonMapper.ofYaml();
    private static final ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();

    @NotNull
    @Schema(title = "Source file URI")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Builder.Default
    @Schema(
        title = "Produce JSONL",
        description = "If true, then one JSON per line. If false, then produce array/object."
    )
    private Property<Boolean> jsonl = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());
        var rJsonl = runContext.render(jsonl).as(Boolean.class).orElse(false);
        var rCharset = runContext.render(charset).as(String.class).orElse(StandardCharsets.UTF_8.name());
        var suffix = rJsonl ? ".jsonl" : ".json";
        File tempFile = runContext.workingDir().createTempFile(suffix).toFile();

        long count;

        try (
            Reader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(rFrom), rCharset));
            Writer writer = new BufferedWriter(new FileWriter(tempFile, Charset.forName(rCharset)), FileSerde.BUFFER_SIZE);
            JsonGenerator jsonGen = JSON_MAPPER.createGenerator(writer)
        ) {
            Iterator<Object> docs = YAML_MAPPER.readerFor(Object.class).readValues(reader);

            if (rJsonl) {
                Flux<Object> flow = Flux.generate(
                    () -> docs,
                    (it, sink) -> {
                        try {
                            if (!it.hasNext()) {
                                sink.complete();
                                return it;
                            }

                            Object doc = it.next();
                            writeJsonlRecord(writer, doc);
                            sink.next(doc);
                        } catch (Exception e) {
                            sink.error(e);
                        }
                        return it;
                    }
                );

                count = flow.count().block();
            }
            else {
                Flux<Object> flow = Flux.generate(
                    () -> new Object[]{docs, null, false},
                    (state, sink) -> {
                        try {
                            return processJsonArrayState(state, jsonGen, sink);
                        } catch (Exception e) {
                            sink.error(e);
                            return state;
                        }
                    }
                );
                count = flow.count().block();
            }
        }

        runContext.metric(Counter.of("records", count));

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private void writeJsonlRecord(Writer writer, Object doc) throws IOException {
        var sw = new StringWriter();
        JSON_MAPPER.writeValue(sw, doc);
        writer.write(sw.toString().stripLeading());
        writer.write("\n");
    }

    private Object[] processJsonArrayState(Object[] state, JsonGenerator jsonGenerator, SynchronousSink<Object> sink) throws IOException {
        Iterator<Object> it = (Iterator<Object>) state[0];
        Object first = state[1];
        boolean isArray = (boolean) state[2];

        if (first == null) {
            if (!it.hasNext()) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeEndObject();
                sink.complete();
                return state;
            }

            first = it.next();
            state[1] = first;

            if (it.hasNext() || first instanceof List) {
                jsonGenerator.writeStartArray();
                isArray = true;
                state[2] = true;
            }

            jsonGenerator.writeObject(first);
            sink.next(first);
            return state;
        }

        if (it.hasNext()) {
            Object next = it.next();
            jsonGenerator.writeObject(next);
            sink.next(next);
            return state;
        }

        if (isArray) {
            jsonGenerator.writeEndArray();
        }

        sink.complete();
        return state;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the output file")
        private final URI uri;
    }
}
