package io.kestra.plugin.serdes.protobuf;

import static io.kestra.core.utils.Rethrow.throwConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.TimeZone;
import java.util.function.Consumer;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.property.URIFetcher;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Convert a Protobuf file into Amazon Ion.", description = """
        This plugin converts binary-encoded Protocol Buffers (Protobuf) data into the Amazon Ion format.

        ### Overview
        The plugin reads one or more Protobuf messages from a binary file or stream,
        decodes them using a provided descriptor and message type name, and then serializes
        the result as Ion data.

        ### Requirements
        - A **descripto file** (`.desc`), generated with `protoc --descriptor_set_out`, that contains
          the compiled Protobuf message definitions.
        - A **type name** (e.g., `com.company.Product`) corresponding to the root message to decode.

        ### Supported Input
        - Single message binary files
        - Streams containing multiple concatenated Protobuf messages

        ### Output
        - Amazon Ion format, either text or binary (depending on configuration)

        ### Example descriptor generation
        ```
        protoc --proto_path=src/main/proto \
               --descriptor_set_out=products.desc \
               src/main/proto/products.proto
        ```

        ### Example Ion output
        Each decoded message is converted to an Ion struct:
        ```
        {
          product_id: "1",
          product_name: "streamline turn-key systems",
          product_category: "Electronics",
          brand: "gomez"
        }
        ```
        """)
@Plugin(examples = {
        @Example(full = true, title = "Convert a Protobuf file to the Amazon Ion format.", code = """
                id: protobuf_to_ion
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://example.com/data/products.pb

                  - id: to_ion
                    type: io.kestra.plugin.serdes.protobuf.ProtobufToIon
                    from: "{{ outputs.http_download.uri }}"
                    descriptorFile: "kestra:///path/to/proto.desc"
                    typeName: com.company.Product
                """)
}, metrics = {
        @Metric(name = "records", description = "Number of Protobuf messages converted", type = Counter.TYPE),
}, aliases = "io.kestra.plugin.serdes.protobuf.ProtobufReader")
public class ProtobufToIon extends Task implements RunnableTask<ProtobufToIon.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson().copy()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS)
            .setTimeZone(TimeZone.getDefault());

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of a temporary result file")
        private final URI uri;
    }

    @NotNull
    @Schema(title = "Source file URI", description = "The URI of the input Protobuf file to read.")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @NotNull
    @Schema(title = "Protobuf Descriptor File", description = "Protobuf descriptor file containing message definitions.")
    @PluginProperty(internalStorageURI = true)
    private Property<String> descriptorFile;

    @NotNull
    @Schema(title = "Fully qualified Protobuf message type name", description = "For example: `com.company.Product`.")
    private Property<String> typeName;

    @Builder.Default
    @Schema(title = "Is the input a stream of length-delimited messages?", description = """
            If true, the input file is expected to contain multiple length-delimited Protobuf messages.
            If false, it will be parsed as a single Protobuf message.
            """)
    private final Property<Boolean> delimited = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Whether to error on unknown fields", description = "If true, an error will be thrown if the input contains unknown fields.")
    private final Property<Boolean> errorOnUnknownFields = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // protobuf descriptor
        InputStream descriptorFileStream = URIFetcher
                .of(runContext.render(descriptorFile).as(String.class).orElseThrow())
                .fetch(runContext);
        String typeName = runContext.render(this.typeName).as(String.class).orElseThrow();
        boolean isDelimited = runContext.render(this.delimited).as(Boolean.class).orElseThrow();
        boolean errorOnUnknownFields = runContext.render(this.errorOnUnknownFields).as(Boolean.class).orElseThrow();

        FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(descriptorFileStream);
        Descriptor messageDescriptor = findMessageDescriptor(descriptorSet, typeName);
        if (messageDescriptor == null) {
            throw new IllegalArgumentException("Message type not found in descriptor: " + typeName);
        }

        try (
                InputStream inputStream = runContext.storage().getFile(from);
                Writer writer = new BufferedWriter(new FileWriter(tempFile, StandardCharsets.UTF_8),
                        FileSerde.BUFFER_SIZE)) {

            Flux<Object> flowable = Flux
                    .create(this.nextMessage(inputStream, messageDescriptor, isDelimited, errorOnUnknownFields),
                            FluxSink.OverflowStrategy.BUFFER);
            Mono<Long> count = FileSerde.writeAll(writer, flowable);

            // metrics & finalize
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output
                .builder()
                .uri(runContext.storage().putFile(tempFile))
                .build();

    }

    private static Descriptor findMessageDescriptor(FileDescriptorSet descriptorSet, String typeName)
            throws Descriptors.DescriptorValidationException {
        for (FileDescriptorProto fileProto : descriptorSet.getFileList()) {
            FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
            String simpleName = typeName.substring(typeName.lastIndexOf('.') + 1);
            Descriptor desc = fileDescriptor.findMessageTypeByName(simpleName);
            if (desc != null) {
                return desc;
            }
        }
        return null;
    }

    private Consumer<FluxSink<Object>> nextMessage(InputStream inputStream, Descriptor messageDescriptor,
            boolean isDelimited, boolean errorOnUnknown) throws IOException {
        ObjectReader objectReader = OBJECT_MAPPER.readerFor(Object.class);

        return throwConsumer(s -> {
            while (true) {
                DynamicMessage message;

                // Read one message
                if (isDelimited) {
                    com.google.protobuf.DynamicMessage.Builder builder = DynamicMessage
                            .newBuilder(messageDescriptor);
                    if (!builder.mergeDelimitedFrom(inputStream)) {
                        break;
                    }
                    message = builder.build();

                } else {
                    message = DynamicMessage.newBuilder(messageDescriptor)
                            .mergeFrom(inputStream)
                            .build();
                }

                if (errorOnUnknown && !message.getUnknownFields().asMap().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Message contains unknown fields: " + message.getUnknownFields().asMap());
                }

                // If we hit EOF or no fields, stop
                if (message == null || message.getAllFields().isEmpty()) {
                    break;
                }

                // Convert protobuf -> JSON
                String json = JsonFormat.printer().print(message);
                Object objects = objectReader.readValue(json);

                if (objects instanceof Collection) {
                    ((Collection<?>) objects)
                            .forEach(s::next);
                } else {
                    s.next(objects);
                }

                // Non-delimited: single message only
                if (!isDelimited) {
                    break;
                }
            }
            s.complete();
        });
    }
}