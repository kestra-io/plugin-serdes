package io.kestra.plugin.serdes.yaml;

import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * Reads YAML documents ({@code ---}-separated) resolving anchors, aliases and merge keys ({@code <<}).
 * Jackson's YAML parser (used by {@code JacksonMapper.ofYaml()}) cannot do this: it turns an alias
 * into a plain string holding the anchor name and leaves {@code <<} as a literal map key, silently
 * corrupting the data. SnakeYAML's {@link SafeConstructor} resolves both correctly and, unlike the
 * default {@code Constructor}, never instantiates arbitrary Java types from YAML tags.
 * <p>
 * {@link LoaderOptions} defaults are kept as-is (50 aliases per collection, 50 levels of nesting,
 * 3 MB per document) to prevent YAML bomb / billion-laughs style alias expansion.
 */
final class YamlDocumentReader {

    private YamlDocumentReader() {
    }

    /**
     * A document whose root is itself a YAML sequence is flattened into one record per element,
     * matching the previous Jackson-based reader's behavior (and the tasks' documented contract:
     * a plain YAML list yields one record per item, not one record holding the whole list).
     */
    static Iterator<Object> readAll(Reader reader) {
        Iterable<Object> documents = safe(new Yaml(new StringTimestampSafeConstructor(new LoaderOptions())).loadAll(reader));

        return StreamSupport.stream(documents.spliterator(), false)
            .flatMap(document -> document instanceof List<?> list ? list.stream().map(o -> (Object) o) : Stream.of(document))
            .iterator();
    }

    private static Iterable<Object> safe(Iterable<Object> delegate) {
        return () -> new SafeIterator(delegate.iterator());
    }

    /**
     * SafeConstructor resolves date/datetime-shaped scalars (e.g. {@code 2024-01-01}) to
     * {@link java.util.Date} by default. The previous Jackson-based reader always kept them as
     * plain strings, and Kestra's date/time handling (KestraDateTimeModule) only customizes
     * java.time types, not java.util.Date — auto-converting would silently reformat dates in
     * YamlToJson output and flip the ION type from string to timestamp in YamlToIon output.
     */
    private static final class StringTimestampSafeConstructor extends SafeConstructor {
        private StringTimestampSafeConstructor(LoaderOptions loaderOptions) {
            super(loaderOptions);
            this.yamlConstructors.put(Tag.TIMESTAMP, this.new ConstructYamlStr());
        }
    }

    /**
     * Translates SnakeYAML's safety-limit exceptions (alias/nesting/size guards) into an actionable
     * message; other parse errors (malformed YAML) already carry line/column context via a Mark and
     * are rethrown unchanged.
     */
    private static final class SafeIterator implements Iterator<Object> {
        private final Iterator<Object> delegate;

        private SafeIterator(Iterator<Object> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (YAMLException e) {
                throw translate(e);
            }
        }

        @Override
        public Object next() {
            try {
                return delegate.next();
            } catch (YAMLException e) {
                throw translate(e);
            }
        }

        private static YAMLException translate(YAMLException e) {
            String message = e.getMessage();
            if (message == null || e.getClass() != YAMLException.class) {
                return e;
            }
            if (
                !message.contains("exceeds the specified max")
                    && !message.contains("Nesting Depth exceeded")
                    && !message.contains("exceeds the limit")
            ) {
                return e;
            }

            return new YAMLException(
                "YAML document rejected: it exceeds SnakeYAML's safety limits for aliases, nesting depth or "
                    + "document size, which usually means excessive anchor/alias expansion (a YAML bomb). "
                    + "Reduce the number of aliases, the nesting depth, or split the document. Original error: "
                    + message,
                e
            );
        }
    }
}
