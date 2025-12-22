package io.kestra.plugin.serdes.protobuf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.property.URIFetcher;
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
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.TimeZone;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Convert a Protobuf file into Amazon Ion.", description = """
        The plugin reads one or more Protobuf messages from a binary file or stream,
        decodes them using a provided descriptor and message type name, and then serializes
        the result as Ion data based on [ProtoJSON Format](https://protobuf.dev/programming-guides/json/).
        It requires the following information regarding the Protobuf message:
        - A **descriptor file** (`.desc`), generated with `--descriptor_set_out`, that contains
          the compiled Protobuf message definitions.
        - A **type name** (e.g., `com.company.Product`) corresponding to the root message to decode.

        Here's an example of how to generate a descriptor file from a Protobuf file using protoc:
        ```
        protoc --proto_path=src/main/proto \
               --descriptor_set_out=products.desc \
               src/main/proto/products.proto
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
        @Metric(name = "records", description = "Number of Protobuf messages converted", type = Counter.TYPE)
})
public class ProtobufToIon extends Task implements RunnableTask<ProtobufToIon.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson().copy()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS).setTimeZone(TimeZone.getDefault());

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
        URI rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // protobuf descriptor
        InputStream rDescriptorFileStream = URIFetcher
                .of(runContext.render(descriptorFile).as(String.class).orElseThrow()).fetch(runContext);
        String rTypeName = runContext.render(this.typeName).as(String.class).orElseThrow();
        boolean rIsDelimited = runContext.render(this.delimited).as(Boolean.class).orElseThrow();
        boolean rErrorOnUnknownFields = runContext.render(this.errorOnUnknownFields).as(Boolean.class).orElseThrow();

        FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(rDescriptorFileStream);
        Descriptor messageDescriptor = ProtobufTools.findMessageDescriptor(descriptorSet, rTypeName);
        if (messageDescriptor == null) {
            throw new IllegalArgumentException("Message type not found in descriptor: " + rTypeName);
        }

        try (InputStream inputStream = runContext.storage().getFile(rFrom);
                Writer writer = new BufferedWriter(new FileWriter(tempFile, StandardCharsets.UTF_8),
                        FileSerde.BUFFER_SIZE)) {

            Flux<Object> flowable = Flux.create(
                    this.nextMessage(inputStream, messageDescriptor, rIsDelimited, rErrorOnUnknownFields),
                    FluxSink.OverflowStrategy.BUFFER);
            Mono<Long> count = FileSerde.writeAll(writer, flowable);

            // metrics & finalize
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output.builder().uri(runContext.storage().putFile(tempFile)).build();
    }

    private Consumer<FluxSink<Object>> nextMessage(InputStream inputStream, Descriptor messageDescriptor,
            boolean isDelimited, boolean errorOnUnknown) throws IOException {
        ObjectReader objectReader = OBJECT_MAPPER.readerFor(Object.class);

        return throwConsumer(s -> {
            while (true) {
                DynamicMessage message;

                // Read one message
                if (isDelimited) {
                    com.google.protobuf.DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
                    if (!builder.mergeDelimitedFrom(inputStream)) {
                        break;
                    }
                    message = builder.build();

                } else {
                    message = DynamicMessage.newBuilder(messageDescriptor).mergeFrom(inputStream).build();
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
                    ((Collection<?>) objects).forEach(s::next);
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
