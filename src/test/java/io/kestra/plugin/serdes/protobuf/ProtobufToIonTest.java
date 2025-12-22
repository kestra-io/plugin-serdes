package io.kestra.plugin.serdes.protobuf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.*;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@KestraTest
class ProtobufToIonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private static File descriptorV10File;
    private static File singleV10File;
    private static File delimitedV10File;
    private static File descriptorV11File;
    private static File singleV11File;
    private static File delimitedV11File;
    private static final String TYPE_NAME = "com.example.Product";

    @BeforeAll
    static void setup() throws Exception {
        descriptorV10File = new File("src/test/resources/protobuf/v1.0/products.desc");
        singleV10File = new File("src/test/resources/protobuf/v1.0/product-single.pb");
        delimitedV10File = new File("src/test/resources/protobuf/v1.0/product-delimited.pb");

        descriptorV11File = new File("src/test/resources/protobuf/v1.1/products.desc");
        singleV11File = new File("src/test/resources/protobuf/v1.1/product-single.pb");
        delimitedV11File = new File("src/test/resources/protobuf/v1.1/product-delimited.pb");

        if (!descriptorV10File.exists()) {
            throw new FileNotFoundException(
                    "Descriptor file not found: " + descriptorV10File.getAbsolutePath());
        }

        // Load descriptor
        FileDescriptorSet descriptorV10Set = FileDescriptorSet.parseFrom(new FileInputStream(descriptorV10File));
        Descriptors.Descriptor descriptorV10 = ProtobufTools.findMessageDescriptor(descriptorV10Set, TYPE_NAME);
        if (descriptorV10 == null) {
            throw new IllegalArgumentException(
                    "Message type not found in descriptor: " + TYPE_NAME);
        }
        FileDescriptorSet descriptorV11Set = FileDescriptorSet.parseFrom(new FileInputStream(descriptorV11File));
        Descriptors.Descriptor descriptorV11 = ProtobufTools.findMessageDescriptor(descriptorV11Set, TYPE_NAME);
        if (descriptorV11 == null) {
            throw new IllegalArgumentException(
                    "Message type not found in descriptor: " + TYPE_NAME);
        }

        // Build sample messages
        Map<String, Object> product1 = Map.of("product_id", "1", "product_name", "streamline",
                "brand", "gomez", "description", "streamline");
        Map<String, Object> product2 = Map.of("product_id", "2", "product_name", "turn-key",
                "brand", "rodriguez", "description", "turn-key");

        // Create files
        createSingleFile(singleV10File, descriptorV10, product1);
        createDelimitedFile(delimitedV10File, descriptorV10, product1, product2);
        createSingleFile(singleV11File, descriptorV11, product1);
        createDelimitedFile(delimitedV11File, descriptorV11, product1, product2);
    }

    @Test
    void testSingleMessage() throws Exception {
        URI sourceV10 = serdesUtils.resourceToStorageObject(singleV10File);
        URI sourceV11 = serdesUtils.resourceToStorageObject(singleV11File);
        URI descriptorV10Uri = serdesUtils.resourceToStorageObject(descriptorV10File);
        URI descriptorV11Uri = serdesUtils.resourceToStorageObject(descriptorV11File);

        for (URI descriptorUri : new URI[] { descriptorV10Uri, descriptorV11Uri }) {
            for (URI source : new URI[] { sourceV10, sourceV11 }) {
                var task = ProtobufToIon.builder().id("protobuf-to-ion-single")
                        .type(ProtobufToIon.class.getName())
                        .from(Property.ofValue(source.toString()))
                        .descriptorFile(Property.ofValue(descriptorUri.toString())) // now a URI
                        .typeName(Property.ofValue(TYPE_NAME)).build();

                var output = task
                        .run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

                var ion = IOUtils.toString(
                        storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()),
                        StandardCharsets.UTF_8);

                assertThat(ion.contains("streamline"), is(true));
                assertThat(ion.contains("gomez"), is(true));
            }
        }
    }

    @Test
    void testDelimitedMessages() throws Exception {
        URI sourceV10 = serdesUtils.resourceToStorageObject(delimitedV10File);
        URI sourceV11 = serdesUtils.resourceToStorageObject(delimitedV11File);
        URI descriptorV10Uri = serdesUtils.resourceToStorageObject(descriptorV10File);
        URI descriptorV11Uri = serdesUtils.resourceToStorageObject(descriptorV11File);

        for (URI descriptorUri : new URI[] { descriptorV10Uri, descriptorV11Uri }) {
            for (URI source : new URI[] { sourceV10, sourceV11 }) {
                var task = ProtobufToIon.builder().id("protobuf-to-ion-delimited")
                        .type(ProtobufToIon.class.getName())
                        .from(Property.ofValue(source.toString()))
                        .descriptorFile(Property.ofValue(descriptorUri.toString())) // now a URI
                        .typeName(Property.ofValue(TYPE_NAME)).delimited(Property.ofValue(true))
                        .build();

                var output = task
                        .run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

                var ion = IOUtils.toString(
                        storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()),
                        StandardCharsets.UTF_8);

                assertThat(ion.contains("streamline"), is(true));
                assertThat(ion.contains("turn-key"), is(true));
            }
        }
    }

    @Test
    void testErrorOnUnknwonMessage() throws Exception {
        URI source = serdesUtils.resourceToStorageObject(delimitedV11File);
        URI descriptorUri = serdesUtils.resourceToStorageObject(descriptorV10File);

        var task = ProtobufToIon.builder().id("protobuf-to-ion-delimited")
                .type(ProtobufToIon.class.getName()).from(Property.ofValue(source.toString()))
                .descriptorFile(Property.ofValue(descriptorUri.toString())) // now
                                                                            // a
                                                                            // URI
                .typeName(Property.ofValue(TYPE_NAME)).delimited(Property.ofValue(true))
                .errorOnUnknownFields(Property.ofValue(true)).build();

        assertThrows(IllegalArgumentException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });
    }

    @Test
    void testFindMessageDescriptor() throws Exception {
        FileDescriptorSet descriptorV11Set = FileDescriptorSet.parseFrom(new FileInputStream(descriptorV11File));
        Descriptor Timestamp1 = ProtobufTools.findMessageDescriptor(descriptorV11Set, "google.protobuf.Timestamp");
        assertEquals(Timestamp1.getFullName(), "google.protobuf.Timestamp");
        Descriptor Timestamp2 = ProtobufTools.findMessageDescriptor(descriptorV11Set, "com.example.Timestamp");
        assertEquals(Timestamp2.getFullName(), "com.example.Timestamp");
        Descriptor NotFound = ProtobufTools.findMessageDescriptor(descriptorV11Set, "Product");
        assertNull(NotFound);
    }

    // ---------- Helpers ----------

    private static void createSingleFile(File output, Descriptors.Descriptor descriptor,
            Map<String, Object> fields) throws IOException {
        try (FileOutputStream out = new FileOutputStream(output)) {
            DynamicMessage msg = buildDynamicMessage(descriptor, fields);
            msg.writeTo(out);
        }
    }

    @SafeVarargs
    private static void createDelimitedFile(File output, Descriptors.Descriptor descriptor,
            Map<String, Object>... messages) throws IOException {
        try (FileOutputStream out = new FileOutputStream(output)) {
            for (Map<String, Object> m : messages) {
                DynamicMessage msg = buildDynamicMessage(descriptor, m);
                msg.writeDelimitedTo(out);
            }
        }
    }

    private static DynamicMessage buildDynamicMessage(Descriptors.Descriptor descriptor,
            Map<String, Object> fields) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (var field : descriptor.getFields()) {
            if (fields.containsKey(field.getName())) {
                builder.setField(field, fields.get(field.getName()));
            }
        }
        return builder.build();
    }
}
