package io.kestra.plugin.serdes.avro;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.validations.DateFormat;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "fsdfdsfdsfsd."
)
@Plugin(
)
public class InferIonToAvro extends Task implements RunnableTask<InferIonToAvro.Output> {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Override
    public InferIonToAvro.Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.workingDir().createTempFile(".avro").toFile();

        // avro writer
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            Reader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE);
        ) {
            var foundFields = new ArrayList<Schema.Field>();
            tryIonLib(runContext.storage().getFile(from));
            debugAsString(runContext.storage().getFile(from));

            var flowable = FileSerde.readAll(inputStream)
                .take(1)// TODO remove
                .doOnNext(row -> {
                    System.out.println(row);
                    // TODO handle array / string
                    foundFields.add(
                        inferField("root_name_to_name", row)
                    );

                });
            var count = flowable.count().block();
            //var inferedSchema = Schema.createRecord("MySchema", "my-doc", "MyNamespace", false, foundFields);
            output.write(foundFields.stream().findFirst().get().schema().toString().getBytes(StandardCharsets.UTF_8));// TODO
            output.flush();
            runContext.logger().warn("Finished for {} records", count);
        }

        return InferIonToAvro.Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private void debugAsString(InputStream in) throws IOException {
        var str = IOUtils.toString(in, StandardCharsets.UTF_8);
        System.out.println(str);
    }

    private void tryIonLib(InputStream in) throws IOException {
        SimpleCatalog myCatalog = new SimpleCatalog();
        IonSystem mySystem = IonSystemBuilder.standard()
            .withCatalog(myCatalog)
            .build();
        var ionReader = mySystem.newReader(in);
        var next = ionReader.next();
        System.out.println("root ion type: " + next);
    }

    @SuppressWarnings("unchecked")
    private Schema.Field inferField(String name, Object node) {
        if (node instanceof Map) {
            var map = (Map<String, Object>) node;
            var inferredFields = new ArrayList<Schema.Field>();
            for (Map.Entry<String, Object> field : map.entrySet()) {
                inferredFields.add(
                    inferField(
                        field.getKey(),
                        field.getValue()
                    )
                );
            }
            return new Schema.Field(
                name,
                Schema.createRecord(
                    name,
                    null,
                    "mynamespace",
                    false,
                    inferredFields
                )
            );
        } else if (node instanceof List) {
            var list = (List<Object>) node;
            if (!list.isEmpty()) {
                var inferredType = inferField(name+"_items", list.get(0));
                return new Schema.Field(
                    name,
                    Schema.createArray(inferredType.schema())
                );
            }
            // TODO handle this case
        } else if (node instanceof byte[]) {
            return new Schema.Field(name, Schema.create(Schema.Type.BYTES));
        } else if (node instanceof String) {
            return new Schema.Field(name, Schema.create(Schema.Type.STRING));
        } else if (node instanceof Integer) {
            return new Schema.Field(name, Schema.create(Schema.Type.INT));
        } else if (node instanceof Float) {
            return new Schema.Field(name, Schema.create(Schema.Type.FLOAT));
        } else if (node instanceof Double) {
            return new Schema.Field(name, Schema.create(Schema.Type.DOUBLE));
        } else if (node instanceof Boolean) {
            return new Schema.Field(name, Schema.create(Schema.Type.LONG));
        } else if (
            Stream.of(Instant.class, ZonedDateTime.class, LocalDateTime.class, OffsetDateTime.class)
                .anyMatch(c -> c.isInstance(node))
        ) {
            return new Schema.Field(name, Schema.createUnion(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
        } else if (node instanceof LocalDate || node instanceof Date) {
            return new Schema.Field(name, Schema.createUnion(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))));
        } else if (node instanceof LocalTime || node instanceof OffsetTime) {
            return new Schema.Field(name, Schema.createUnion(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))));
        }
        // TODO logical type
        // TODO misc types
        throw new IllegalArgumentException("Unhandled node " + name + " with content: " + node);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of a temporary result file"
        )
        private URI uri;
    }

    protected static boolean contains(List<String> list, String data) {
        return list.stream().anyMatch(s -> s.equalsIgnoreCase(data));
    }

    @Builder.Default
    protected final List<String> trueValues = Arrays.asList("t", "true", "enabled", "1", "on", "yes");

    @Builder.Default
    protected final List<String> falseValues = Arrays.asList("f", "false", "disabled", "0", "off", "no", "");

    @Builder.Default
    protected final List<String> nullValues = Arrays.asList(
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "1.#IND",
        "1.#QNAN",
        "NA",
        "n/a",
        "nan",
        "null"
    );

    @Builder.Default
    @DateFormat
    protected final String dateFormat = "yyyy-MM-dd[XXX]";

    @Builder.Default
    @DateFormat
    protected final String timeFormat = "HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @DateFormat
    protected final String datetimeFormat = "yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    protected final Character decimalSeparator = '.';

    private String generateName(){
        return "generated_"+UUID.randomUUID().toString().replace("-", "_");
    };
}
