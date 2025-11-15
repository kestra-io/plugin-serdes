package io.kestra.plugin.serdes.xml;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.json.XMLParserConfiguration;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an XML file into ION."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an XML file to the Amazon Ion format.",
            code = """
                id: xml_to_ion
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/xml/products.xml

                  - id: to_ion
                    type: io.kestra.plugin.serdes.xml.XmlToIon
                    from: "{{ outputs.http_download.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", description = "Number of records converted", type = Counter.TYPE),
    },
    aliases = "io.kestra.plugin.serdes.xml.XmlReader"
)
public class XmlToIon extends Task implements RunnableTask<XmlToIon.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Schema(
        title = "XPath use to query in the XML file."
    )
    private Property<String> query;

    @Schema(
        title = "XML parser configuration."
    )
    private ParserConfiguration parserConfiguration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            Reader input = new BufferedReader(
                new InputStreamReader(
                    runContext.storage().getFile(from),
                    runContext.render(charset).as(String.class).orElseThrow()
                ),
                FileSerde.BUFFER_SIZE
            );
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            XMLParserConfiguration xmlParserConfiguration = new XMLParserConfiguration();
            if (parserConfiguration != null) {
                var renderedParserConfig = runContext.render(parserConfiguration.getForceList()).asList(String.class);
                xmlParserConfiguration = xmlParserConfiguration.withForceList(new HashSet<>(renderedParserConfig));
            }
            JSONObject jsonObject = XML.toJSONObject(input, xmlParserConfiguration);

            Object result = result(jsonObject, runContext);

            if (result instanceof JSONObject) {
                Map<String, Object> map = ((JSONObject) result).toMap();
                FileSerde.write(output, map);
                runContext.metric(Counter.of("records", map.size()));
            } else if (result instanceof JSONArray) {
                List<Object> list = ((JSONArray) result).toList();
                list.forEach(throwConsumer(o -> {
                    FileSerde.write(output, o);
                }));
                runContext.metric(Counter.of("records", list.size()));
            } else {
                FileSerde.write(output, result);
            }

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private Object result(JSONObject jsonObject, RunContext runContext) throws IllegalVariableEvaluationException {
        var renderedQuery = runContext.render(this.query).as(String.class);
        var logger = runContext.logger();
        if (renderedQuery.isPresent()) {
            try {
                return jsonObject.query(renderedQuery.get());
            } catch (Exception e) {
                logger.debug("JsonObject query failed, Object maybe null or empty.");
                return null;
            }
        } else {
            return jsonObject;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;
    }

    @Builder
    @Data
    @Schema(title = "XML parser configuration.")
    public static class ParserConfiguration {
        @Schema(
            title = "List of XML tags that must be parsed as lists."
        )
        private Property<List<String>> forceList;
    }
}
