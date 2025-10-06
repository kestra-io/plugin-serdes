package io.kestra.plugin.serdes.markdown;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
public class MarkdownToTextTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void should_convert_markdown_to_plain_text() throws Exception {
        // Given
        String markdown = """
            # Berlin News

            **Cirque du Soleil** announces new show in *Berlin*.

            Read more at [reuters.com](https://reuters.com)

            - Item A
            - Item B
            """;

        var runContext = getRunContext(markdown);

        var task = MarkdownToText.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String text = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            // check expected content
            assertThat(text, containsString("Berlin News"));
            assertThat(text, containsString("Cirque du Soleil announces new show in Berlin."));
            assertThat(text, containsString("Read more at reuters.com"));
            assertThat(text, containsString("- Item A"));
            assertThat(text, containsString("- Item B"));

            // check no markdown remains
            assertThat(text, not(containsString("**")));
            assertThat(text, not(containsString("*Berlin*")));
            assertThat(text, not(containsString("[")));
            assertThat(text, not(containsString("](")));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    @Test
    void should_handle_empty_markdown() throws Exception {
        var runContext = getRunContext("");

        var task = MarkdownToText.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        var output = task.run(runContext);

        assertThat(storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String text = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(text.trim(), is(""));
        }
    }

    private RunContext getRunContext(String markdownContent) {
        Map<String, String> kestraVars = new HashMap<>();
        URI filePath;
        try {
            filePath = storageInterface.put(
                MAIN_TENANT,
                null,
                URI.create("/" + IdUtils.create() + ".md"),
                new ByteArrayInputStream(markdownContent.getBytes(StandardCharsets.UTF_8))
            );
            kestraVars.put("file", filePath.toString());
        } catch (Exception e) {
            fail("Unable to prepare input file: " + e.getMessage());
            return null;
        }
        return runContextFactory.of(ImmutableMap.copyOf(kestraVars));
    }
}
