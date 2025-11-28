package io.kestra.plugin.serdes.yaml;

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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class JsonToYamlTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storage;

    private URI put(String content) throws Exception {
        return storage.put(MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".json"),
            new ByteArrayInputStream(content.getBytes(UTF_8))
        );
    }

    private String read(URI uri) throws Exception {
        return new String(storage.get(MAIN_TENANT, null, uri).readAllBytes(), StandardCharsets.UTF_8)
            .replace("\r\n", "\n");
    }

    @Test
    void jsonl_to_yaml_docs() throws Exception {
        URI src = put("""
            {"a":1}
            {"b":2}
            """);

        var task = JsonToYaml.builder()
            .from(Property.ofValue(src.toString()))
            .jsonl(Property.ofValue(true))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));
        String result = read(out.getUri());

        assertThat(result, containsString("---\na: 1"));
        assertThat(result, containsString("---\nb: 2"));
    }

    @Test
    void json_to_yaml_single() throws Exception {
        URI src = put("{\"a\":1}");

        var task = JsonToYaml.builder()
            .jsonl(Property.ofValue(false))
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));
        String result = read(out.getUri());

        assertThat(result.strip(), is("a: 1"));
    }
}
