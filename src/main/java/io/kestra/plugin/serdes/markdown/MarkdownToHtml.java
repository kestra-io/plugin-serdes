package io.kestra.plugin.serdes.markdown;

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
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert a Markdown file into an HTML file.",
    description = "Turns Markdown into HTML syntax suitable for embedding into HTML contexts like email templates."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert Markdown text into HTML syntax.",
            code = """
                id: markdown_to_html
                namespace: company.team

                tasks:
                  - id: output_markdown
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      markdown: |
                        # Hello
                        This is **bold** and [link](https://kestra.io)

                  - id: write_markdown
                    type: io.kestra.plugin.core.storage.Write
                    content: "{{ outputs.output_markdown.values.markdown }}"
                    extension: ".md"

                  - id: to_html
                    type: io.kestra.plugin.serdes.markdown.MarkdownToHtml
                    from: "{{ outputs.write_markdown.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "bytes", description = "Number of bytes generated", type = Counter.TYPE),
    }
)
public class MarkdownToHtml extends Task implements RunnableTask<MarkdownToHtml.Output> {
    @NotNull
    @Schema(title = "Source file URI")
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(title = "Charset to use for input/output", description = "Default is UTF-8.")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var rCharset = Charset.forName(runContext.render(this.charset).as(String.class).orElse(StandardCharsets.UTF_8.name()));

        var tempFile = runContext.workingDir().createTempFile(".html").toFile();

        try (var inputStream = runContext.storage().getFile(rFrom);
             var reader = new InputStreamReader(inputStream, rCharset);
             var writer = new OutputStreamWriter(new FileOutputStream(tempFile), rCharset)) {

            var parser = Parser.builder().build();
            var document = parser.parseReader(reader);
            var renderer = HtmlRenderer.builder().escapeHtml(false).build();

            var html = renderer.render(document);
            writer.write(html);

            runContext.metric(Counter.of("bytes", html.length()));
        }

        return Output.builder().uri(runContext.storage().putFile(tempFile)).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the generated HTML file")
        private final URI uri;
    }
}
