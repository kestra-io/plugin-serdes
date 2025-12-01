package io.kestra.plugin.serdes.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class YamlToJsonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storage;

    private String read(URI uri) throws Exception {
        return new String(storage.get(MAIN_TENANT, null, uri).readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");
    }

    private URI put(String yaml) throws Exception {
        return storage.put(MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".yaml"),
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void jsonl_multipleDocs() throws Exception {
        String yaml = """
            ---
            a: 1
            ---
            b: 2
            """;

        URI src = put(yaml);

        var task = YamlToJson.builder()
            .from(Property.ofValue(src.toString()))
            .jsonl(Property.ofValue(true))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));

        String result = read(out.getUri());
        assertThat(result, is("{\"a\":1}\n{\"b\":2}\n"));
    }

    @Test
    void json_array_multipleDocs() throws Exception {
        String yaml = """
            ---
            a: 1
            ---
            b: 2
            """;
        URI src = put(yaml);

        var task = YamlToJson.builder()
            .from(Property.ofValue(src.toString()))
            .jsonl(Property.ofValue(false))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));

        String result = read(out.getUri());

        var mapper = new ObjectMapper();
        assertThat(mapper.readTree(result), is(mapper.readTree("[{\"a\":1},{\"b\":2}]")));
    }

    @Test
    void json_singleDocument() throws Exception {
        String yaml = "a: 1";
        URI src = put(yaml);

        var task = YamlToJson.builder()
            .jsonl(Property.ofValue(false))
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));
        String result = read(out.getUri());

        assertThat(result.strip(), is("{\"a\":1}"));
    }

    @Test
    void json_listRoot() throws Exception {
        String yaml = """
            - a
            - b
            """;
        URI src = put(yaml);

        var task = YamlToJson.builder()
            .jsonl(Property.ofValue(false))
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));
        String result = read(out.getUri());

        var mapper = new ObjectMapper();
        assertThat(
            mapper.readTree(result),
            is(mapper.readTree("[\"a\",\"b\"]"))
        );
    }
}
