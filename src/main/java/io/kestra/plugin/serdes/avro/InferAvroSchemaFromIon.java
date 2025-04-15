package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URI;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "InferAvroSchemaFromIon."
)
@Plugin(
)
public class InferAvroSchemaFromIon extends Task implements RunnableTask<InferAvroSchemaFromIon.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Override
    public InferAvroSchemaFromIon.Output run(RunContext runContext) throws Exception {
        var tempAvroSchemaFile = runContext.workingDir().createTempFile(".avsc").toFile();

        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            var inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            var output = new BufferedOutputStream(new FileOutputStream(tempAvroSchemaFile), FileSerde.BUFFER_SIZE);
        ) {
            new InferAvroSchema().inferAvroSchemaFromIon(inputStream, output);
            output.flush();
        }

        return InferAvroSchemaFromIon.Output
            .builder()
            .uri(runContext.storage().putFile(tempAvroSchemaFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }
}
