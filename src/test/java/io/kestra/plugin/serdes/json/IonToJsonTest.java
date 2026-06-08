package io.kestra.plugin.serdes.json;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
public class IonToJsonTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void should_transform_ion_to_json_without_annotations() throws Exception {
        var ion = """
            {dn:"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2",base64::"TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ=","Melusine lover as well"],someOtherAttribute:["perhaps 2","perhapsAgain 2"]}}
            """;
        var expectedJsonWithoutAnnotation = """
            {"dn":"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com","attributes":{"description":["Some description 2","TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ=","Melusine lover as well"],"someOtherAttribute":["perhaps 2","perhapsAgain 2"]}}
            """;

        var runContext = getRunContext(ion);
        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .shouldKeepAnnotations(Property.ofValue(false))
            .build();
        var output = task.run(runContext);

        assertEquality(expectedJsonWithoutAnnotation, output.getUri());
    }

    @Test
    void should_transform_ion_to_json_with_annotations() throws Exception {
        var ion = """
            {dn:"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2",base64::"TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ=","Melusine lover as well"],someOtherAttribute:["perhaps 2","perhapsAgain 2"]}}
            """;

        String expectedJsonWithAnnotation = """
            {"dn":"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com","attributes":{"description":["Some description 2",{"ion_annotations":["base64"], "value":"TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ="},"Melusine lover as well"],"someOtherAttribute":["perhaps 2","perhapsAgain 2"]}}
            """;

        var runContext = getRunContext(ion);
        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .shouldKeepAnnotations(Property.ofValue(true))
            .build();
        var output = task.run(runContext);

        assertEquality(expectedJsonWithAnnotation, output.getUri());
    }

    @Test
    void should_stream_large_ion_without_memory_issue() throws Exception {
        var builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < 200000; i++) {
            if (i > 0)
                builder.append(",");
            builder.append("{\"id\":").append(i).append(",\"name\":\"Item").append(i).append("\"}");
        }
        builder.append("]");

        var runContext = getRunContext(builder.toString());
        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .shouldKeepAnnotations(Property.ofValue(false))
            .newLine(Property.ofValue(false))
            .build();

        var output = task.run(runContext);
        assertThat(storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));
    }

    @Test
    void should_parse_binary_ion_correctly() throws Exception {
        // FileSerde.write produces binary ION (BVM E0 01 00 EA prefix).
        // The old Reader-based path would corrupt this via UTF-8 decoding.
        var tempFile = File.createTempFile("ion_binary_", ".ion");
        try (var output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.of("id", 1, "name", "alice"),
                ImmutableMap.of("id", 2, "name", "bob")
            ).forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }

        URI uri = storageInterface.put(MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
        Map<String, Object> variables = ImmutableMap.of("file", uri.toString());
        var runContext = runContextFactory.of(variables);

        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .shouldKeepAnnotations(Property.ofValue(false))
            .build();
        var output = task.run(runContext);

        assertThat(storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));
        try (var stream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            var result = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(result, containsString("\"id\""));
            assertThat(result, containsString("\"alice\""));
            assertThat(result, containsString("\"bob\""));
        }
    }

    @Test
    void should_handle_empty_binary_ion_without_throwing() throws Exception {
        // An empty binary ION file (just the BVM, no records) must produce an empty output.
        var tempFile = File.createTempFile("ion_empty_", ".ion");
        try (var output = new FileOutputStream(tempFile)) {
            // write nothing: empty file
        }

        URI uri = storageInterface.put(MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
        Map<String, Object> variables = ImmutableMap.of("file", uri.toString());
        var runContext = runContextFactory.of(variables);

        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .shouldKeepAnnotations(Property.ofValue(false))
            .build();

        assertDoesNotThrow(() -> task.run(runContext));
    }

    private RunContext getRunContext(String ionContent) {
        Map<String, String> kestraPath = new HashMap<>();
        URI filePath;
        try {
            filePath = storageInterface.put(
                MAIN_TENANT,
                null,
                URI.create("/" + IdUtils.create() + ".ion"),
                new ByteArrayInputStream(ionContent.getBytes())
            );
            kestraPath.put("file", filePath.toString());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("Unable to load input file.");
            return null;
        }
        return runContextFactory.of(ImmutableMap.copyOf(kestraPath));
    }

    private void assertEquality(String expected, URI file) {
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, file), is(true));

        try (InputStream streamResult = storageInterface.get(MAIN_TENANT, null, file)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");

            System.out.println("Got :\n" + result);
            System.out.println("Expecting :\n" + expected);

            var mapper = new ObjectMapper();

            var actualNode = mapper.readTree(result);
            var expectedNode = mapper.readTree(expected);

            assertThat("Result should match the reference", actualNode.equals(expectedNode));

        } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("Unable to load results files.");
        }
    }
}
