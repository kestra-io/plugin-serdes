package io.kestra.plugin.serdes.yaml;

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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    title = "Convert a YAML file into ION.",
    description = "Converts YAML documents into Amazon Ion. Each YAML document becomes one Ion value."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert YAML to Ion",
            code = """
                id: yaml_to_ion
                namespace: company.team

                tasks:
                  - id: write_yaml
                    type: io.kestra.plugin.core.storage.Write
                    extension: yaml
                    content: |
                      ---
                      id: 1
                      name: Apple
                      price: 1.2
                      ---
                      id: 2
                      name: Banana
                      price: 0.9

                  - id: convert
                    type: io.kestra.plugin.serdes.yaml.YamlToIon
                    from: "{{ outputs.write_yaml.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of YAML documents converted", type = Counter.TYPE)
    }
)
public class YamlToIon extends Task implements RunnableTask<YamlToIon.Output> {
    private static final ObjectMapper YAML_MAPPER = JacksonMapper.ofYaml();

    @NotNull
    @PluginProperty(internalStorageURI = true)
    @Schema(title = "Source file URI")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());
        var rCharset = runContext.render(charset).as(String.class).orElse(StandardCharsets.UTF_8.name());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        long count;

        try (
            Reader yamlReader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(rFrom), rCharset), FileSerde.BUFFER_SIZE);
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            Iterator<Object> docs = YAML_MAPPER.readerFor(Object.class).readValues(yamlReader);

            Flux<Object> flow = Flux.generate(
                () -> docs,
                (iterator, sink) -> {
                    try {
                        if (iterator.hasNext()) {
                            Object doc = iterator.next();
                            FileSerde.write(outputStream, doc);
                            sink.next(doc);
                        } else {
                            sink.complete();
                        }
                    } catch (Exception e) {
                        sink.error(e);
                    }
                    return iterator;
                }
            );

            count = Mono.from(flow.count()).block();
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