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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
public class MarkdownToHtmlTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void should_convert_markdown_to_html() throws Exception {
        // Given
        String markdown = """
            # Hello Kestra

            This is **bold** and *italic* text with a [link](https://kestra.io)

            - Item 1
            - Item 2
            """;

        var runContext = getRunContext(markdown);

        var task = MarkdownToHtml.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String html = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(html, containsString("<h1>Hello Kestra</h1>"));
            assertThat(html, containsString("<strong>bold</strong>"));
            assertThat(html, containsString("<em>italic</em>"));
            assertThat(html, containsString("<a href=\"https://kestra.io\">link</a>"));
            assertThat(html, containsString("<li>Item 1</li>"));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
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
