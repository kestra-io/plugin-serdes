package io.kestra.plugin.serdes.xml;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.json.XMLParserConfiguration;

import jakarta.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read a XML file and write it to an ion serialized data file."
)
@Slf4j
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert a xml file to ion format.",
            code = """     
id: xml_to_ion
namespace: dev

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
    aliases = "io.kestra.plugin.serdes.xml.XmlReader"
)
public class XmlToIon extends Task implements RunnableTask<XmlToIon.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty
    private final String charset = StandardCharsets.UTF_8.name();

    @Schema(
        title = "XPath use to query in the XML file."
    )
    @PluginProperty
    private String query;

    @Schema(
        title = "XML parser configuration."
    )
    @PluginProperty
    private ParserConfiguration parserConfiguration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            XMLParserConfiguration xmlParserConfiguration = new XMLParserConfiguration();
            if (parserConfiguration != null) {
                xmlParserConfiguration = xmlParserConfiguration.withForceList(parserConfiguration.getForceList());
            }
            JSONObject jsonObject = XML.toJSONObject(input, xmlParserConfiguration);

            Object result = result(jsonObject);

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

    private Object result(JSONObject jsonObject) {
        if (this.query != null) {
            try {
                return jsonObject.query(this.query);
            } catch (Exception e) {
                log.debug("JsonObject query failed, Object maybe null or empty.");
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
        @PluginProperty
        private Set<String> forceList;
    }
}
