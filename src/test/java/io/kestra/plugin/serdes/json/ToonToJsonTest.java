package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ToonToJsonTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private ToonToJson.Output convert(File sourceFile) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ToonToJson task = ToonToJson.builder()
            .id(ToonToJson.class.getSimpleName())
            .type(ToonToJson.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        return task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
    }

    private String readResult(URI uri) throws Exception {
        return IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, uri), StandardCharsets.UTF_8);
    }

    private JsonNode readJson(String content) throws Exception {
        return MAPPER.readTree(content);
    }

    @Test
    void simpleObject() throws Exception {
        String toon = """
            id: 123
            name: Ada
            active: true
            """;

        File temp = File.createTempFile("object_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "id": 123,
              "name": "Ada",
              "active": true
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
        assertThat(FilenameUtils.getExtension(output.getUri().getPath()), is("json"));
    }

    @Test
    void tabularArray() throws Exception {
        String toon = """
            users[2]{id,name,active}:
              1,Alice,true
              2,Bob,false
            """;

        File temp = File.createTempFile("tabular_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "users": [
                {"id": 1, "name": "Alice", "active": true},
                {"id": 2, "name": "Bob", "active": false}
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void mixedArray() throws Exception {
        String toon = """
            items[3]:
              - 1
              - a: x
              - hello
            """;

        File temp = File.createTempFile("mixed_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "items": [
                1,
                {"a": "x"},
                "hello"
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void nestedObjects() throws Exception {
        String toon = """
            server:
              host: localhost
              port: 8080
              tags[2]: web,api
            """;

        File temp = File.createTempFile("nested_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "server": {
                "host": "localhost",
                "port": 8080,
                "tags": ["web", "api"]
              }
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void quotingAndSpecialChars() throws Exception {
        String toon = """
            urls[2]{id,url}:
              1,"http://a:b"
              2,"https://example.com?q=a:b"
            """;

        File temp = File.createTempFile("quote_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "urls": [
                {"id": 1, "url": "http://a:b"},
                {"id": 2, "url": "https://example.com?q=a:b"}
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void emptyArrayAndNull() throws Exception {
        String toon = """
            tags[0]:
            note: null
            """;

        File temp = File.createTempFile("empty_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "tags": [],
              "note": null
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void unicodeAndEmoji() throws Exception {
        String toon = """
            message: Hello 世界 👋
            tags[3]: 🎉,🎊,🎈
            """;

        File temp = File.createTempFile("unicode_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "message": "Hello 世界 👋",
              "tags": ["🎉", "🎊", "🎈"]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }
}
