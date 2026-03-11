package io.kestra.plugin.serdes.xml;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.json.XMLParserConfiguration;

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
        title = "Path selector to stream matching elements from the XML file.",
        description = """
            When set, uses StAX streaming to extract elements matching the given path
            (e.g. `/catalog/book`). Each matching element is written as a separate ION record.
            When not set, the entire XML file is parsed into a single ION record."""
    )
    private Property<String> query;

    @Schema(
        title = "XML parser configuration."
    )
    private ParserConfiguration parserConfiguration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var rCharset = runContext.render(charset).as(String.class).orElseThrow();
        var rQuery = runContext.render(this.query).as(String.class);

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        var xmlParserConfiguration = new XMLParserConfiguration();
        if (parserConfiguration != null) {
            var rParserConfig = runContext.render(parserConfiguration.getForceList()).asList(String.class);
            xmlParserConfiguration = xmlParserConfiguration.withForceList(new HashSet<>(rParserConfig));
        }

        if (rQuery.isPresent()) {
            runStreaming(runContext, from, rCharset, rQuery.get(), xmlParserConfiguration, tempFile);
        } else {
            runBatch(runContext, from, rCharset, xmlParserConfiguration, tempFile);
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    private void runBatch(RunContext runContext, URI from, String charset, XMLParserConfiguration xmlParserConfiguration, File tempFile) throws Exception {
        try (
            Reader input = new BufferedReader(
                new InputStreamReader(runContext.storage().getFile(from), charset),
                FileSerde.BUFFER_SIZE
            );
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var jsonObject = XML.toJSONObject(input, xmlParserConfiguration);
            var result = unwrapRootArray(jsonObject);

            if (result instanceof JSONArray array) {
                var list = array.toList();
                list.forEach(throwConsumer(o -> FileSerde.write(output, o)));
                runContext.metric(Counter.of("records", list.size()));
            } else if (result instanceof JSONObject obj) {
                var map = obj.toMap();
                FileSerde.write(output, map);
                runContext.metric(Counter.of("records", map.size()));
            } else {
                FileSerde.write(output, result);
            }

            output.flush();
        }
    }

    /**
     * Unwraps the root XML structure to extract the inner array of records when the XML
     * follows the common pattern produced by {@link IonToXml}: {@code <items><item>...</item></items>}.
     * <p>
     * Handles two patterns:
     * <ul>
     * <li>{@code {"root": [...]}} — root element directly contains an array</li>
     * <li>{@code {"root": {"child": [...]}}} — root element wraps a single child element containing an array</li>
     * </ul>
     * Falls back to returning the original JSONObject if the structure doesn't match.
     */
    private Object unwrapRootArray(JSONObject jsonObject) {
        if (jsonObject.length() != 1) {
            return jsonObject;
        }

        var rootKey = jsonObject.keys().next();
        var rootValue = jsonObject.get(rootKey);

        if (rootValue instanceof JSONArray) {
            return rootValue;
        }

        if (rootValue instanceof JSONObject innerObj && innerObj.length() == 1) {
            var innerKey = innerObj.keys().next();
            var innerValue = innerObj.get(innerKey);
            if (innerValue instanceof JSONArray) {
                return innerValue;
            }
        }

        return jsonObject;
    }

    private void runStreaming(RunContext runContext, URI from, String charset, String query, XMLParserConfiguration xmlParserConfiguration, File tempFile) throws Exception {
        // Parse query: "/catalog/book" → parentSegments=["catalog"], elementName="book"
        var segments = query.replaceFirst("^/", "").split("/");
        var parentSegments = new String[segments.length - 1];
        System.arraycopy(segments, 0, parentSegments, 0, segments.length - 1);
        var elementName = segments[segments.length - 1];

        var factory = XMLInputFactory.newInstance();
        // Disable external entities for security
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

        var recordCount = 0;

        try (
            InputStream is = runContext.storage().getFile(from);
            BufferedInputStream bis = new BufferedInputStream(is, FileSerde.BUFFER_SIZE);
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            XMLStreamReader reader;
            try {
                reader = factory.createXMLStreamReader(bis, charset);
            } catch (XMLStreamException e) {
                // Empty or unparseable XML file — produce empty output
                runContext.logger().debug("Failed to parse XML stream, file may be empty.");
                output.flush();
                return;
            }

            try {
                boolean parentFound;
                try {
                    parentFound = navigateToParent(reader, parentSegments);
                } catch (XMLStreamException e) {
                    // Empty or malformed XML — produce empty output
                    runContext.logger().debug("Failed to navigate XML stream, file may be empty.");
                    output.flush();
                    return;
                }
                if (!parentFound) {
                    output.flush();
                    return;
                }

                // Now we are positioned on the parent element's START_ELEMENT.
                // Iterate over its children looking for matching elements.
                int depth = 0;
                while (reader.hasNext()) {
                    int event = reader.next();
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        if (depth == 0 && reader.getLocalName().equals(elementName)) {
                            String xmlFragment = readElementAsXml(reader);
                            JSONObject parsed = XML.toJSONObject(xmlFragment, xmlParserConfiguration);
                            // Unwrap the outer element key
                            Object inner = parsed.opt(elementName);
                            if (inner instanceof JSONObject) {
                                FileSerde.write(output, ((JSONObject) inner).toMap());
                            } else if (inner instanceof JSONArray) {
                                List<Object> list = ((JSONArray) inner).toList();
                                for (Object o : list) {
                                    FileSerde.write(output, o);
                                    recordCount++;
                                }
                                continue;
                            } else {
                                FileSerde.write(output, inner);
                            }
                            recordCount++;
                        } else {
                            // Non-matching child: skip its entire subtree
                            skipElement(reader);
                        }
                    } else if (event == XMLStreamConstants.END_ELEMENT) {
                        if (depth == 0) {
                            // End of the parent element
                            break;
                        }
                        depth--;
                    }
                }
            } finally {
                reader.close();
            }

            output.flush();
        }

        runContext.metric(Counter.of("records", recordCount));
    }

    /**
     * Advance the StAX reader to the start element matching the parent path.
     * For example, parentSegments=["catalog"] will position on &lt;catalog&gt;.
     * Returns false if the parent path was not found.
     */
    private boolean navigateToParent(XMLStreamReader reader, String[] parentSegments) throws Exception {
        for (String segment : parentSegments) {
            boolean found = false;
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    if (reader.getLocalName().equals(segment)) {
                        found = true;
                        break;
                    } else {
                        skipElement(reader);
                    }
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    /**
     * Read the current element (the reader is positioned on its START_ELEMENT)
     * and return its complete XML subtree as a string, including the element tag itself.
     * After this method returns, the reader is positioned just after the matching END_ELEMENT.
     */
    private String readElementAsXml(XMLStreamReader reader) throws Exception {
        var sb = new StringBuilder();
        var localName = reader.getLocalName();

        // Write opening tag with attributes
        sb.append('<').append(localName);
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            sb.append(' ').append(reader.getAttributeLocalName(i))
                .append("=\"").append(escapeXmlAttribute(reader.getAttributeValue(i))).append('"');
        }
        sb.append('>');

        int depth = 1;
        while (reader.hasNext() && depth > 0) {
            int event = reader.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT:
                    depth++;
                    sb.append('<').append(reader.getLocalName());
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        sb.append(' ').append(reader.getAttributeLocalName(i))
                            .append("=\"").append(escapeXmlAttribute(reader.getAttributeValue(i))).append('"');
                    }
                    sb.append('>');
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    depth--;
                    if (depth > 0) {
                        sb.append("</").append(reader.getLocalName()).append('>');
                    }
                    break;
                case XMLStreamConstants.CHARACTERS:
                case XMLStreamConstants.SPACE:
                    sb.append(escapeXmlContent(reader.getText()));
                    break;
                case XMLStreamConstants.CDATA:
                    sb.append("<![CDATA[").append(reader.getText()).append("]]>");
                    break;
                default:
                    break;
            }
        }
        // Close the outer element
        sb.append("</").append(localName).append('>');

        return sb.toString();
    }

    /**
     * Skip the current element and all its children.
     * The reader must be positioned on a START_ELEMENT.
     * After this method returns, the reader is positioned just after the matching END_ELEMENT.
     */
    private void skipElement(XMLStreamReader reader) throws Exception {
        int depth = 1;
        while (reader.hasNext() && depth > 0) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                depth++;
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                depth--;
            }
        }
    }

    private static String escapeXmlContent(String text) {
        return text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;");
    }

    private static String escapeXmlAttribute(String text) {
        return text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;");
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
