package io.kestra.plugin.serdes.json;

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

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class JsonToToonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private JsonToToon.Output convert(File sourceFile) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToToon task = JsonToToon.builder()
            .id(JsonToToon.class.getSimpleName())
            .type(JsonToToon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        return task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
    }

    private String readResult(URI uri) throws Exception {
        return IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, uri), StandardCharsets.UTF_8);
    }

    @Test
    void simpleObject() throws Exception {
        String json = """
            {
              "id": 123,
              "name": "Ada",
              "active": true
            }
            """;

        File temp = File.createTempFile("object_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            id: 123
            name: Ada
            active: true
            """;

        assertThat(result.trim(), is(expected.trim()));
        assertThat(FilenameUtils.getExtension(output.getUri().getPath()), is("toon"));
    }

    @Test
    void tabularArray() throws Exception {
        String json = """
            {
              "users": [
                {"id": 1, "name": "Alice", "active": true},
                {"id": 2, "name": "Bob", "active": false}
              ]
            }
            """;

        File temp = File.createTempFile("tabular_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            users[2]{id,name,active}:
              1,Alice,true
              2,Bob,false
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void mixedArray() throws Exception {
        String json = """
            {
              "items": [
                1,
                {"a": "x"},
                "hello"
              ]
            }
            """;

        File temp = File.createTempFile("mixed_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            items[3]:
              - 1
              - a: x
              - hello
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void nestedObjects() throws Exception {
        String json = """
            {
              "server": {
                "host": "localhost",
                "port": 8080,
                "tags": ["web", "api"]
              }
            }
            """;

        File temp = File.createTempFile("nested_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            server:
              host: localhost
              port: 8080
              tags[2]: web,api
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void quotingAndSpecialChars() throws Exception {
        String json = """
            {
              "urls": [
                {"id": 1, "url": "http://a:b"},
                {"id": 2, "url": "https://example.com?q=a:b"}
              ]
            }
            """;

        File temp = File.createTempFile("quote_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            urls[2]{id,url}:
              1,"http://a:b"
              2,"https://example.com?q=a:b"
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void emptyArrayAndNull() throws Exception {
        String json = """
            {
              "tags": [],
              "note": null
            }
            """;

        File temp = File.createTempFile("empty_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            tags[0]:
            note: null
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void unicodeAndEmoji() throws Exception {
        String json = """
            {
              "message": "Hello 世界 👋",
              "tags": ["🎉", "🎊", "🎈"]
            }
            """;

        File temp = File.createTempFile("unicode_", ".json");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        JsonToToon.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            message: Hello 世界 👋
            tags[3]: 🎉,🎊,🎈
            """;

        assertThat(result.trim(), is(expected.trim()));
    }

    @Test
    void large() throws Exception {
        // Generate a large JSON array of 10K objects
        int count = 10_000;
        File temp = File.createTempFile("huge_", ".json");

        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write("{\"items\": [\n");
            for (int i = 0; i < count; i++) {
                w.write("{\"id\": " + i + ", \"name\": \"User" + i + "\", \"active\": true}");
                if (i < count - 1) {
                    w.write(",\n");
                }
            }
            w.write("\n]}");
        }

        JsonToToon.Output output = convert(temp);

        // read as streaming to avoid using too much memory
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri());
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            // The first line must be a tabular header
            String firstLine = br.readLine();
            assertThat(firstLine, is("items[" + count + "]{id,name,active}:"));

            // Skip forward but track last non-null line
            String line, lastNonEmpty = null;
            while ((line = br.readLine()) != null) {
                if (!line.isBlank()) {
                    lastNonEmpty = line;
                }
            }

            // Last row of the tabular should be (count-1),User(count-1),true
            String expectedLast = "  " + (count - 1) + ",User" + (count - 1) + ",true";
            assertThat(lastNonEmpty, is(expectedLast));
        }
    }
}
