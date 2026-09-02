package io.kestra.plugin.serdes.yaml;

import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

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
        Iterable<Object> documents = new Yaml(new SafeConstructor(new LoaderOptions())).loadAll(reader);

        return StreamSupport.stream(documents.spliterator(), false)
            .flatMap(document -> document instanceof List<?> list ? list.stream().map(o -> (Object) o) : Stream.of(document))
            .iterator();
    }
}
