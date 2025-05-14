package io.kestra.plugin.serdes.avro;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.avro.infer.InferAvroSchema;
import io.swagger.v3.oas.annotations.media.Schema;
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
@Schema(
    title = "Try to infer an Avro schema from a ION file."
)
@Plugin(
    aliases = "io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon",
    beta = true
)
public class InferAvroSchemaFromIon extends Task implements RunnableTask<InferAvroSchemaFromIon.Output> {
    @NotNull
    @Schema(
        title = "ION source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @NotNull
    @Builder.Default
    @Schema(
        title = "The number of rows that will be scanned; the larger the number of rows, the more precise the output schema will be."
    )
    private Property<Integer> numberOfRowsToScan = Property.of(100);

    @Override
    public InferAvroSchemaFromIon.Output run(RunContext runContext) throws Exception {
        var tempAvroSchemaFile = runContext.workingDir().createTempFile(".avsc").toFile();

        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            var inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            var output = new BufferedOutputStream(new FileOutputStream(tempAvroSchemaFile), FileSerde.BUFFER_SIZE);
        ) {
            new InferAvroSchema(
                runContext.render(this.numberOfRowsToScan).as(Integer.class).orElseThrow()
            ).inferAvroSchemaFromIon(inputStream, output);
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
        @Schema(
            title = "Avro schema file URI"
        )
        private URI uri;
    }
}
