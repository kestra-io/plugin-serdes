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

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an ION file into YAML format."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Download a CSV file and convert it to YAML format.",
            code = """
                id: ion_to_yaml
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.http_download.uri }}"

                  - id: to_yaml
                    type: io.kestra.plugin.serdes.yaml.IonToYaml
                    from: "{{ outputs.convert.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    }
)
public class IonToYaml extends Task implements RunnableTask<IonToYaml.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI rFrom = new URI(runContext.render(from).as(String.class).orElseThrow());
        Charset outputCharset = Charset.forName(runContext.render(charset).as(String.class).orElse(StandardCharsets.UTF_8.name()));

        File tempFile = runContext.workingDir().createTempFile(".yaml").toFile();
        ObjectMapper yamlMapper = JacksonMapper.ofYaml();

        long count = 0;

        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(rFrom)), FileSerde.BUFFER_SIZE);
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile, outputCharset), FileSerde.BUFFER_SIZE)
        ) {

            Flux<Object> flux = FileSerde.readAll(reader);
            var it = flux.toIterable().iterator();

            if (it.hasNext()) {
                Object first = it.next();
                boolean multi = it.hasNext();

                if (!multi) {
                    yamlMapper.writeValue(writer, first);
                    writer.write("\n");
                    count = 1;
                } else {
                    writer.write("---\n");
                    yamlMapper.writeValue(writer, first);
                    writer.write("\n");
                    count++;

                    while (it.hasNext()) {
                        writer.write("---\n");
                        yamlMapper.writeValue(writer, it.next());
                        writer.write("\n");
                        count++;
                    }
                }
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
        private final URI uri;
    }
}