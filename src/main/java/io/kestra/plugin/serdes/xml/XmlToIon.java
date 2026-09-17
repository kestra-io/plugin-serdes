package io.kestra.plugin.serdes.xml;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        Without a `query`, the entire file is parsed into a single ION record. \
        When `query` is set (e.g., `/catalog/book`), uses StAX streaming to \
        extract each matching element as a separate ION record — suitable for \
        large files. External entity resolution is disabled for security. \
        Scalar text values keep their XML lexical form when they would otherwise \
        be corrupted by numeric coercion (e.g. `25E2568` is not mistaken for \
        scientific notation and overflowed to `Infinity`); see `parserConfiguration` \
        to control number/boolean typing explicitly."""
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
            When not set, the entire XML file is parsed into a single ION record."""
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "XML parser configuration",
        description = """
            Controls how repeated elements and scalar text values are typed. By default, \
            values that look numeric or boolean (e.g. `true`, `12.5`) are converted to the \
            matching ION type, except when doing so would silently corrupt the value — for \
            example `25E2568` looks like scientific notation but is far outside the range \
            of a `double`, so it is kept as the string `"25E2568"` instead of becoming \
            `Infinity`."""
    )
    @PluginProperty(group = "advanced")
    private ParserConfiguration parserConfiguration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        var rCharset = runContext.render(charset).as(String.class).orElseThrow();
        var rQuery = runContext.render(this.query).as(String.class);

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // Never let org.json coerce scalars at parse time — the lexical form would be destroyed
        // before we can guard it. We re-apply the same typing ourselves in coerce() (see #412).
        var xmlParserConfiguration = new XMLParserConfiguration()
            .withKeepNumberAsString(true)
            .withKeepBooleanAsString(true);
        var rForceString = new HashSet<String>();
        var rKeepNumberAsString = false;
        var rKeepBooleanAsString = false;
        if (parserConfiguration != null) {
            var rForceList = runContext.render(parserConfiguration.getForceList()).asList(String.class);
            xmlParserConfiguration = xmlParserConfiguration.withForceList(new HashSet<>(rForceList));
            rForceString.addAll(runContext.render(parserConfiguration.getForceString()).asList(String.class));
            rKeepNumberAsString = runContext.render(parserConfiguration.getKeepNumberAsString()).as(Boolean.class).orElse(false);
            rKeepBooleanAsString = runContext.render(parserConfiguration.getKeepBooleanAsString()).as(Boolean.class).orElse(false);
        }

        long count;
        if (rQuery.isPresent()) {
            count = runStreaming(runContext, from, rCharset, rQuery.get(), xmlParserConfiguration, tempFile, rForceString, rKeepNumberAsString, rKeepBooleanAsString);
        } else {
            count = runBatch(runContext, from, rCharset, xmlParserConfiguration, tempFile, rForceString, rKeepNumberAsString, rKeepBooleanAsString);
        }

        return Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(count)
            .build();
    }

    private long runBatch(RunContext runContext, URI from, String charset, XMLParserConfiguration xmlParserConfiguration, File tempFile, Set<String> forceString, boolean keepNumberAsString, boolean keepBooleanAsString) throws Exception {
        try (
            Reader input = new BufferedReader(
                new InputStreamReader(runContext.storage().getFile(from), charset),
                FileSerde.BUFFER_SIZE
            );
            OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var jsonObject = XML.toJSONObject(input, xmlParserConfiguration);
            coerce(jsonObject, forceString, keepNumberAsString, keepBooleanAsString, runContext);
            var result = unwrapRootArray(jsonObject);

            long count;
            if (result instanceof JSONArray array) {
                var list = array.toList();
                list.forEach(throwConsumer(o -> FileSerde.write(output, o)));
                count = list.size();
            } else if (result instanceof JSONObject obj) {
                var map = obj.toMap();
                FileSerde.write(output, map);
                count = 1L;
            } else {
                FileSerde.write(output, result);
                count = 1L;
            }

            runContext.metric(Counter.of("records", count));
            output.flush();
            return count;
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

    private long runStreaming(RunContext runContext, URI from, String charset, String query, XMLParserConfiguration xmlParserConfiguration, File tempFile, Set<String> forceString, boolean keepNumberAsString, boolean keepBooleanAsString) throws Exception {
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
                            coerce(parsed, forceString, keepNumberAsString, keepBooleanAsString, runContext);
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

    /**
     * Re-applies org.json's own scalar typing ({@link XML#stringToValue}) on a tree parsed with
     * {@code keepNumberAsString}/{@code keepBooleanAsString} enabled, so numeric/boolean coercion
     * happens under our control instead of destroying the original lexical form at parse time.
     * Walks element text nodes AND attributes, since org.json represents both as plain keys of
     * the same {@link JSONObject}.
     */
    private static void coerce(JSONObject jsonObject, Set<String> forceString, boolean keepNumberAsString, boolean keepBooleanAsString, RunContext runContext) {
        for (var key : new ArrayList<>(jsonObject.keySet())) {
            jsonObject.put(key, coerceValue(jsonObject.get(key), key, forceString, keepNumberAsString, keepBooleanAsString, runContext));
        }
    }

    private static Object coerceValue(Object value, String elementName, Set<String> forceString, boolean keepNumberAsString, boolean keepBooleanAsString, RunContext runContext) {
        if (value instanceof JSONObject nested) {
            coerce(nested, forceString, keepNumberAsString, keepBooleanAsString, runContext);
            return nested;
        }
        if (value instanceof JSONArray array) {
            for (var i = 0; i < array.length(); i++) {
                array.put(i, coerceValue(array.get(i), elementName, forceString, keepNumberAsString, keepBooleanAsString, runContext));
            }
            return array;
        }
        if (value instanceof String text) {
            return coerceScalar(text, elementName, forceString, keepNumberAsString, keepBooleanAsString, runContext);
        }
        return value;
    }

    private static Object coerceScalar(String text, String elementName, Set<String> forceString, boolean keepNumberAsString, boolean keepBooleanAsString, RunContext runContext) {
        if (forceString.contains(elementName)) {
            return text;
        }

        var value = XML.stringToValue(text);

        if (value instanceof Boolean) {
            return keepBooleanAsString ? text : value;
        }

        // stringToValue returns Integer/Long/BigInteger for all-digit text and BigDecimal for
        // decimal/exponent notation — any of them can narrow to Infinity or 0 on doubleValue(),
        // e.g. a ~309+ digit integer overflows just like "25E2568" does (see #412).
        if (value instanceof Number number) {
            if (keepNumberAsString) {
                return text;
            }
            var asDouble = number.doubleValue();
            var overflows = Double.isInfinite(asDouble);
            var underflows = asDouble == 0.0 && !isZero(number);
            if (overflows || underflows) {
                runContext.logger().debug(
                    "XML element '{}' value '{}' would narrow to {} as a double, keeping the original string instead",
                    elementName, text, overflows ? "Infinity" : "0"
                );
                return text;
            }
            return number;
        }

        return value;
    }

    private static boolean isZero(Number number) {
        if (number instanceof BigDecimal bigDecimal) {
            return bigDecimal.compareTo(BigDecimal.ZERO) == 0;
        }
        if (number instanceof BigInteger bigInteger) {
            return bigInteger.signum() == 0;
        }
        return number.longValue() == 0;
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

        @Schema(
            title = "List of XML tags that must always be kept as strings",
            description = """
                Element and attribute names listed here are never converted to a number or a \
                boolean, regardless of their text content. Use this for identifiers that \
                happen to look numeric (lot numbers, SKUs, ZIP codes) so they never get \
                reformatted. Namespaced elements must be prefixed the same way they appear \
                in the source XML (e.g. `ns:code`)."""
        )
        private Property<List<String>> forceString;

        @Builder.Default
        @Schema(
            title = "Whether to keep every numeric-looking value as a string",
            description = """
                When `true`, no scalar text value is ever converted to an ION number. \
                `forceString` still takes precedence for the names it lists, this flag \
                only widens the same behavior to every element and attribute. Default \
                value is `false`."""
        )
        private final Property<Boolean> keepNumberAsString = Property.ofValue(false);

        @Builder.Default
        @Schema(
            title = "Whether to keep every boolean-looking value as a string",
            description = """
                When `true`, `true`/`false` text values are never converted to an ION \
                boolean. `forceString` still takes precedence for the names it lists, \
                this flag only widens the same behavior to every element and attribute. \
                Default value is `false`."""
        )
        private final Property<Boolean> keepBooleanAsString = Property.ofValue(false);
    }
}
