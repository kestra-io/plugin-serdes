package io.kestra.plugin.serdes.markdown;

import io.kestra.core.models.annotations.Example;
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
import org.commonmark.node.*;
import org.commonmark.parser.Parser;

import java.io.BufferedReader;
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
    title = "Convert a Markdown file into plain text.",
    description = "Removes Markdown formatting and outputs plain text, using CommonMark parser."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert Markdown text into plain text.",
            code = """
                id: markdown_to_text
                namespace: company.team

                tasks:
                  - id: output_markdown
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      markdown: |
                        ## Berlin News
                        - **Cirque du Soleil** announces show
                        - Read more at [reuters.com](https://reuters.com)

                  - id: write_markdown
                    type: io.kestra.plugin.core.storage.Write
                    content: "{{ outputs.output_markdown.values.markdown }}"
                    extension: ".md"

                  - id: to_text
                    type: io.kestra.plugin.serdes.markdown.MarkdownToText
                    from: "{{ outputs.write_markdown.uri }}"
                """
        )
    }
)
public class MarkdownToText extends Task implements RunnableTask<MarkdownToText.Output> {
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

        var tempFile = runContext.workingDir().createTempFile(".txt").toFile();

        String markdown;
        try (var inputStream = runContext.storage().getFile(rFrom);
             var reader = new BufferedReader(new InputStreamReader(inputStream, rCharset))) {

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            markdown = sb.toString();
        }

        String plainText = toPlainText(markdown);

        try (var writer = new OutputStreamWriter(new FileOutputStream(tempFile), rCharset)) {
            writer.write(plainText);
        }

        runContext.metric(Counter.of("bytes", plainText.length()));

        return Output.builder().uri(runContext.storage().putFile(tempFile)).build();
    }

    private String toPlainText(String markdown) {
        if (markdown == null) return "";

        Parser parser = Parser.builder().build();
        Node document = parser.parse(markdown);
        StringBuilder sb = new StringBuilder();

        document.accept(new AbstractVisitor() {
            @Override
            public void visit(Text text) {
                sb.append(text.getLiteral());
            }

            @Override
            public void visit(SoftLineBreak softLineBreak) {
                sb.append(" ");
            }

            @Override
            public void visit(HardLineBreak hardLineBreak) {
                sb.append("\n");
            }

            @Override
            public void visit(Link link) {
                // keep the text but ignore the URL
                visitChildren(link);
            }

            @Override
            public void visit(Paragraph paragraph) {
                visitChildren(paragraph);
                sb.append("\n");
            }

            @Override
            public void visit(Heading heading) {
                visitChildren(heading);
                sb.append("\n");
            }

            @Override
            public void visit(ListItem listItem) {
                sb.append("- ");
                visitChildren(listItem);
                sb.append("\n");
            }
        });

        return sb.toString().trim();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the generated plain text file")
        private final URI uri;
    }
}
