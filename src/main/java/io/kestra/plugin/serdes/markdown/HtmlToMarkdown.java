package io.kestra.plugin.serdes.markdown;

import com.vladsch.flexmark.html2md.converter.FlexmarkHtmlConverter;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;



@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an HTML file or string into Markdown format.",
    description = "This task converts HTML content into Markdown, which is essential for LLM processing, documentation automation, and data cleaning pipelines. HTML is often too verbose for Large Language Models (LLMs) and consumes unnecessary tokens. This task allows you to build clean RAG (Retrieval-Augmented Generation) pipelines directly within Kestra."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Fetch a web page and convert it to Markdown.",
            code = """
                id: web_to_markdown_pipeline
                namespace: company.team

                tasks:
                  - id: fetch_html
                    type: io.kestra.plugin.core.http.Request
                    uri: "https://kestra.io/docs"

                  - id: write_html
                    type: io.kestra.plugin.core.storage.Write
                    content: "{{ outputs.fetch_html.body }}"
                    extension: ".html"

                  - id: convert_to_md
                    type: io.kestra.plugin.serdes.markdown.HtmlToMarkdown
                    from: "{{ outputs.write_html.uri }}"

                  - id: log_result
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.convert_to_md.uri }}"
                """
        ),
        @Example(
            title = "Convert HTML file to Markdown with custom options.",
            code = """
                id: html_to_md_custom
                namespace: company.team

                tasks:
                  - id: write_html
                    type: io.kestra.plugin.core.storage.Write
                    content: "<h1>Hello</h1><p>This is <strong>bold</strong> text.</p>"
                    extension: ".html"

                  - id: convert
                    type: io.kestra.plugin.serdes.markdown.HtmlToMarkdown
                    from: "{{ outputs.write_html.uri }}"
                    ignoreTags:
                      - script
                      - style
                      - nav
                """
        )
    },
    metrics = {
        @Metric(name = "bytes", description = "Number of bytes generated", type = Counter.TYPE),
    }
)
public class HtmlToMarkdown extends Task implements RunnableTask<HtmlToMarkdown.Output> {
    @NotNull
    @Schema(title = "Source file URI")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "List of HTML tags to ignore during conversion.",
        description = "Tags specified in this list will be skipped during the conversion process. Common tags to ignore include 'script', 'style', 'nav', etc."
    )
    private Property<List<String>> ignoreTags;

    @Schema(
        title = "Base URI for resolving relative links.",
        description = "When provided, relative URLs in the HTML (e.g., href and src attributes) will be resolved to absolute URLs using this base URI."
    )
    private Property<String> baseUri;

    @Builder.Default
    @Schema(title = "Charset to use for input/output.", description = "Default is UTF-8.")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var rCharset = Charset.forName(runContext.render(this.charset).as(String.class).orElse(StandardCharsets.UTF_8.name()));
        
        String htmlContent;
        
        try (var inputStream = runContext.storage().getFile(rFrom);
             var reader = new BufferedReader(new InputStreamReader(inputStream, rCharset))) {
            
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            htmlContent = sb.toString();
        }

        // Pre-process HTML with jsoup if needed
        org.jsoup.nodes.Document doc;
        if (this.baseUri != null || this.ignoreTags != null) {
            String baseUriValue = null;
            if (this.baseUri != null) {
                baseUriValue = runContext.render(this.baseUri).as(String.class).orElse(null);
            }
            
            doc = org.jsoup.Jsoup.parse(htmlContent, baseUriValue != null ? baseUriValue : "");
            
            // Apply ignore tags if specified
            if (this.ignoreTags != null) {
                var rIgnoreTags = runContext.render(this.ignoreTags).asList(String.class);
                if (!rIgnoreTags.isEmpty()) {
                    for (String tag : rIgnoreTags) {
                        doc.select(tag).remove();
                    }
                }
            }
            
            // Apply base URI if specified
            if (baseUriValue != null && !baseUriValue.isEmpty()) {
                doc.setBaseUri(baseUriValue);
                doc.select("a[href]").forEach(link -> {
                    String absUrl = link.absUrl("href");
                    if (!absUrl.isEmpty()) {
                        link.attr("href", absUrl);
                    }
                });
                doc.select("img[src]").forEach(img -> {
                    String absUrl = img.absUrl("src");
                    if (!absUrl.isEmpty()) {
                        img.attr("src", absUrl);
                    }
                });
            }
            
            htmlContent = doc.html();
        }

        // Configure FlexmarkHtmlConverter with ATX headings
        var converter = FlexmarkHtmlConverter.builder()
            .build();
        String markdown = converter.convert(htmlContent);

        // Write to temp file
        var tempFile = runContext.workingDir().createTempFile(".md").toFile();
        
        try (var writer = new OutputStreamWriter(new FileOutputStream(tempFile), rCharset)) {
            writer.write(markdown);
        }

        runContext.metric(Counter.of("bytes", markdown.length()));

        return Output.builder().uri(runContext.storage().putFile(tempFile)).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the generated Markdown file in Kestra's internal storage.")
        private final URI uri;
    }
}
