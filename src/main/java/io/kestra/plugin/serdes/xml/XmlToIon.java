package io.kestra.plugin.serdes.xml;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
    title = "Convert an XML file to the Amazon ION format",
    description = """
        Without a `query`, the whole document is inspected: if the root element has \
        exactly one repeated, complex child element (e.g. `<catalog><book>...</book>\
        <book>...</book></catalog>`), each occurrence of that child becomes its own \
        flat ION record — this also applies when there is only a single occurrence, \
        so the output shape does not depend on record count. Otherwise, the whole \
        document is parsed into a single nested ION record. Set `unwrapRootCollection` \
        to `false` to always get a single nested record. When `query` is set (e.g., \
        `/catalog/book`), uses StAX streaming to extract each matching element as a \
        separate ION record — suitable for large files. External entity resolution is \
        disabled for security."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an XML file to the Amazon ION format.",
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
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty(group = "processing")
    private final Property<String> charset = Property.ofValue(StandardCharsets.UTF_8.name());

    @Schema(
        title = "Path selector to stream matching elements from the XML file",
        description = """
            When set, uses StAX streaming to extract elements matching the given path
            (e.g. `/catalog/book`). Each matching element is written as a separate ION record.
            When not set, the root's single repeated child element (if any) is unwrapped into
            separate ION records; see `unwrapRootCollection`."""
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Builder.Default
    @Schema(
        title = "Whether to unwrap the root element's repeated child into individual records",
        description = """
            Only used when `query` is not set. When the root element has exactly one \
            distinct, complex child element name (an element with attributes or child \
            elements of its own) and no meaningful text of its own, each occurrence of \
            that child is written as a separate, flat ION record — regardless of whether \
            there is one occurrence or several, so the output shape is stable. \
            Set to `false` to always parse the whole document into a single nested ION \
            record instead. This is useful for config-shaped XML such as \
            `<config><database><host>x</host></database></config>`, which is structurally \
            ambiguous with a one-record collection and would otherwise lose the \
            `database` nesting level. Default value is `true`."""
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> unwrapRootCollection = Property.ofValue(true);

    @Schema(
        title = "XML parser configuration"
    )
    @PluginProperty(group = "advanced")
    private ParserConfiguration parserConfiguration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var rCharset = runContext.render(charset).as(String.class).orElseThrow();
        var rQuery = runContext.render(this.query).as(String.class);
        var rUnwrapRootCollection = runContext.render(this.unwrapRootCollection).as(Boolean.class).orElse(true);

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        var xmlParserConfiguration = new XMLParserConfiguration();
        if (parserConfiguration != null) {
            var rParserConfig = runContext.render(parserConfiguration.getForceList()).asList(String.class);
            xmlParserConfiguration = xmlParserConfiguration.withForceList(new HashSet<>(rParserConfig));
        }

        long count;
        if (rQuery.isPresent()) {
            count = runStreaming(runContext, from, rCharset, rQuery.get(), xmlParserConfiguration, tempFile);
        } else {
            count = runBatch(runContext, from, rCharset, xmlParserConfiguration, rUnwrapRootCollection, tempFile);
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(count)
            .build();
    }

    private long runBatch(RunContext runContext, URI from, String charset, XMLParserConfiguration xmlParserConfiguration, boolean unwrapRootCollection, File tempFile) throws Exception {
        var collectionChildName = unwrapRootCollection ? detectRootCollectionChild(runContext, from, charset) : null;

        var effectiveParserConfiguration = xmlParserConfiguration;
        if (collectionChildName != null) {
            var forceList = new HashSet<>(xmlParserConfiguration.getForceList());
            forceList.add(collectionChildName);
            effectiveParserConfiguration = xmlParserConfiguration.withForceList(forceList);
        }

        try (
            Reader input = new BufferedReader(
                new InputStreamReader(runContext.storage().getFile(from), charset),
                FileSerde.BUFFER_SIZE
            );
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var jsonObject = XML.toJSONObject(input, effectiveParserConfiguration);

            long count;
            if (collectionChildName != null) {
                var rootKey = jsonObject.keys().next();
                var rootValue = jsonObject.getJSONObject(rootKey);
                var list = rootValue.getJSONArray(collectionChildName).toList();
                list.forEach(throwConsumer(o -> FileSerde.write(output, o)));
                count = list.size();
            } else {
                var map = jsonObject.toMap();
                FileSerde.write(output, map);
                count = 1L;
            }

            runContext.metric(Counter.of("records", count));
            output.flush();
            return count;
        }
    }

    /**
     * Structurally detects whether the root element is a wrapper around a repeated record
     * collection, e.g. {@code <catalog><book>...</book><book>...</book></catalog>}, so that
     * a single occurrence and several occurrences of the same child element produce the same
     * ION output shape (see #371).
     * <p>
     * The root qualifies as a collection when ALL of the following hold:
     * <ul>
     * <li>it has exactly one distinct direct child element name;</li>
     * <li>it has no meaningful (non-whitespace) text content of its own;</li>
     * <li>that child element is complex (it has attributes or child elements of its own),
     * so a scalar leaf like {@code <root><value>5</value></root>} is never unwrapped.</li>
     * </ul>
     * Returns the qualified child element name (including its namespace prefix, if any, to
     * match the key {@link XML#toJSONObject} would produce) to unwrap, or {@code null} if the
     * root does not match this pattern.
     */
    private String detectRootCollectionChild(RunContext runContext, URI from, String charset) throws Exception {
        var factory = XMLInputFactory.newInstance();
        // Disable external entities for security
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

        try (
            InputStream is = runContext.storage().getFile(from);
            BufferedInputStream bis = new BufferedInputStream(is, FileSerde.BUFFER_SIZE)
        ) {
            XMLStreamReader reader;
            try {
                reader = factory.createXMLStreamReader(bis, charset);
            } catch (XMLStreamException e) {
                return null;
            }

            try {
                while (reader.hasNext() && reader.next() != XMLStreamConstants.START_ELEMENT) {
                    // skip prolog / DOCTYPE / comments until the root element
                }
                if (reader.getEventType() != XMLStreamConstants.START_ELEMENT) {
                    return null;
                }

                var childNames = new LinkedHashSet<String>();
                var complexChild = false;
                var hasRootText = false;

                while (reader.hasNext()) {
                    int event = reader.next();
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        childNames.add(qualifiedName(reader));
                        var hasAttributes = reader.getAttributeCount() > 0;
                        var hasNestedElement = skipElementTrackingNestedElement(reader);
                        if (hasAttributes || hasNestedElement) {
                            complexChild = true;
                        }
                    } else if (event == XMLStreamConstants.END_ELEMENT) {
                        // Children are fully skipped above, so this can only be the root's own end tag.
                        break;
                    } else if ((event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA)
                        && !reader.getText().isBlank()) {
                        hasRootText = true;
                    }
                }

                if (childNames.size() == 1 && !hasRootText && complexChild) {
                    return childNames.iterator().next();
                }
                return null;
            } catch (XMLStreamException e) {
                return null;
            } finally {
                reader.close();
            }
        }
    }

    private static String qualifiedName(XMLStreamReader reader) {
        var prefix = reader.getPrefix();
        return prefix == null || prefix.isEmpty() ? reader.getLocalName() : prefix + ":" + reader.getLocalName();
    }

    /**
     * Skip the current element and all its children, like {@link #skipElement}, but also
     * report whether it contains at least one nested child element.
     * The reader must be positioned on a START_ELEMENT.
     */
    private boolean skipElementTrackingNestedElement(XMLStreamReader reader) throws Exception {
        var hasNestedElement = false;
        int depth = 1;
        while (reader.hasNext() && depth > 0) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                depth++;
                hasNestedElement = true;
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                depth--;
            }
        }
        return hasNestedElement;
    }

    private long runStreaming(RunContext runContext, URI from, String charset, String query, XMLParserConfiguration xmlParserConfiguration, File tempFile) throws Exception {
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
                return 0L;
            }

            try {
                boolean parentFound;
                try {
                    parentFound = navigateToParent(reader, parentSegments);
                } catch (XMLStreamException e) {
                    // Empty or malformed XML — produce empty output
                    runContext.logger().debug("Failed to navigate XML stream, file may be empty.");
                    output.flush();
                    return 0L;
                }
                if (!parentFound) {
                    output.flush();
                    return 0L;
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
        return recordCount;
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

        @Schema(title = "The number of records converted")
        private long size;
    }

    @Builder
    @Data
    @Schema(title = "XML parser configuration")
    public static class ParserConfiguration {
        @Schema(
            title = "List of XML tags that must be parsed as lists"
        )
        private Property<List<String>> forceList;
    }
}
