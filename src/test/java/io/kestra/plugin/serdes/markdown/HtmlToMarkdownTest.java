package io.kestra.plugin.serdes.markdown;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
public class HtmlToMarkdownTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void should_convert_html_to_markdown_from_storage() throws Exception {
        // Given
        String html = "<h1>Hello Kestra</h1><p>This is <strong>bold</strong> and <em>italic</em> text with a <a href=\"https://kestra.io\">link</a></p><ul><li>Item 1</li><li>Item 2</li></ul>";

        var runContext = getRunContext(html);

        var task = HtmlToMarkdown.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String markdown = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(markdown, containsString("# Hello Kestra"));
            assertThat(markdown, containsString("**bold**"));
            assertThat(markdown, containsString("*italic*"));
            assertThat(markdown, containsString("[link](https://kestra.io)"));
            assertThat(markdown, containsString("Item 1"));
            assertThat(markdown, containsString("Item 2"));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    @Test
    void should_convert_html_to_markdown_from_file() throws Exception {
        // Given
        String html = """
            <!DOCTYPE html>
            <html>
            <head><title>Test</title></head>
            <body>
                <h1>Hello Kestra</h1>
                <p>This is a test document with <strong>bold</strong> text.</p>
                <ul>
                    <li>First item</li>
                    <li>Second item</li>
                </ul>
            </body>
            </html>
            """;

        var runContext = getRunContext(html);

        var task = HtmlToMarkdown.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String markdown = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(markdown, containsString("# Hello Kestra"));
            assertThat(markdown, containsString("**bold**"));
            assertThat(markdown, containsString("First item"));
            assertThat(markdown, containsString("Second item"));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    @Test
    void should_convert_html_table_to_markdown() throws Exception {
        // Given
        String html = """
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Age</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Alice</td>
                        <td>30</td>
                    </tr>
                    <tr>
                        <td>Bob</td>
                        <td>25</td>
                    </tr>
                </tbody>
            </table>
            """;

        var runContext = getRunContext(html);

        var task = HtmlToMarkdown.builder()
            .from(Property.ofExpression("{{ file }}"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String markdown = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(markdown, containsString("Name"));
            assertThat(markdown, containsString("Age"));
            assertThat(markdown, containsString("Alice"));
            assertThat(markdown, containsString("Bob"));
            assertThat(markdown, containsString("|")); // Tables use pipe separators
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    @Test
    void should_ignore_specified_tags() throws Exception {
        // Given
        String html = """
            <h1>Main Content</h1>
            <p>This is visible content.</p>
            <script>console.log('This should be removed');</script>
            <style>body { color: red; }</style>
            <nav><a href="/link">Navigation</a></nav>
            <p>More visible content.</p>
            """;

        var runContext = getRunContext(html);

        var task = HtmlToMarkdown.builder()
            .from(Property.ofExpression("{{ file }}"))
            .ignoreTags(Property.ofValue(List.of("script", "style", "nav")))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String markdown = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(markdown, containsString("Main Content"));
            assertThat(markdown, containsString("visible content"));
            assertThat(markdown, not(containsString("console.log")));
            assertThat(markdown, not(containsString("color: red")));
            assertThat(markdown, not(containsString("Navigation")));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    @Test
    void should_resolve_relative_urls_with_base_uri() throws Exception {
        // Given
        String html = """
            <h1>Test Document</h1>
            <p>Check out <a href="/docs/guide">our guide</a>.</p>
            <img src="/images/logo.png" alt="Logo" />
            """;

        var runContext = getRunContext(html);

        var task = HtmlToMarkdown.builder()
            .from(Property.ofExpression("{{ file }}"))
            .baseUri(Property.ofValue("https://kestra.io"))
            .build();

        // When
        var output = task.run(runContext);

        // Then
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, output.getUri()), is(true));

        try (InputStream resultStream = storageInterface.get(MAIN_TENANT, null, output.getUri())) {
            String markdown = new String(resultStream.readAllBytes(), StandardCharsets.UTF_8);

            assertThat(markdown, containsString("https://kestra.io/docs/guide"));
            assertThat(markdown, containsString("https://kestra.io/images/logo.png"));
        } catch (Exception e) {
            fail("Unable to read output file: " + e.getMessage());
        }
    }

    private RunContext getRunContext(String htmlContent) {
        Map<String, String> kestraVars = new HashMap<>();
        URI filePath;
        try {
            filePath = storageInterface.put(
                MAIN_TENANT,
                null,
                URI.create("/" + IdUtils.create() + ".html"),
                new ByteArrayInputStream(htmlContent.getBytes(StandardCharsets.UTF_8))
            );
            kestraVars.put("file", filePath.toString());
        } catch (Exception e) {
            fail("Unable to prepare input file: " + e.getMessage());
            return null;
        }
        return runContextFactory.of(ImmutableMap.copyOf(kestraVars));
    }
}
