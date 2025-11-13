package io.kestra.plugin.serdes.json;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class JsonToJsonlTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    /**
     * Test converting a JSON array to JSONL format
     */
    @Test
    void shouldConvertJsonArrayToJsonl() throws Exception {
        String jsonArray = """
            [
                {"id": 1, "name": "Product 1", "price": 19.99},
                {"id": 2, "name": "Product 2", "price": 29.99},
                {"id": 3, "name": "Product 3", "price": 39.99}
            ]
            """;

        // Create input file
        URI inputUri = createInputFile(jsonArray);

        // Create task
        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When - Execute the task
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then - Verify output
        assertNotNull(output);
        assertNotNull(output.getUri());

        // Read and verify output content
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(3, lines.size());

        // Verify each line is valid JSON
        assertThat(lines.get(0), containsString("\"id\":1"));
        assertThat(lines.get(0), containsString("\"name\":\"Product 1\""));
        assertThat(lines.get(0), containsString("\"price\":19.99"));

        assertThat(lines.get(1), containsString("\"id\":2"));
        assertThat(lines.get(2), containsString("\"id\":3"));
    }

    /**
     * Test converting a compact JSON array (single line) to JSONL
     */
    @Test
    void shouldConvertCompactJsonArrayToJsonl() throws Exception {
        String jsonArray = "[{\"id\":1,\"name\":\"Item 1\"},{\"id\":2,\"name\":\"Item 2\"}]";

        URI inputUri = createInputFile(jsonArray);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(2, lines.size());
        assertThat(lines.get(0), containsString("\"id\":1"));
        assertThat(lines.get(1), containsString("\"id\":2"));
    }

    /**
     * Test converting already JSONL format (should normalize)
     */
    @Test
    void shouldNormalizeExistingJsonl() throws Exception {
        String jsonl = """
            {"id":1,"name":"Item 1","status":"active"}
            {"id":2,"name":"Item 2","status":"inactive"}
            {"id":3,"name":"Item 3","status":"active"}
            """;

        URI inputUri = createInputFile(jsonl);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(3, lines.size());

        // Verify content is preserved
        assertThat(lines.get(0), containsString("\"id\":1"));
        assertThat(lines.get(1), containsString("\"id\":2"));
        assertThat(lines.get(2), containsString("\"id\":3"));
    }

    /**
     * Test converting a single JSON object to JSONL
     */
    @Test
    void shouldConvertSingleJsonObjectToJsonl() throws Exception {
        // Given - Single JSON object (multiline)
        String singleObject = """
            {
                "id": 100,
                "name": "Single Product",
                "description": "A single product object",
                "metadata": {
                    "created": "2024-01-01",
                    "updated": "2024-01-15"
                }
            }
            """;

        URI inputUri = createInputFile(singleObject);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(1, lines.size());
        assertThat(lines.get(0), containsString("\"id\":100"));
        assertThat(lines.get(0), containsString("\"name\":\"Single Product\""));
    }

    /**
     * Test with complex nested JSON array
     */
    @Test
    void shouldHandleNestedJsonStructures() throws Exception {
        String complexJson = """
            [
                {
                    "id": 1,
                    "user": {
                        "name": "John Doe",
                        "email": "[email protected]"
                    },
                    "items": ["item1", "item2", "item3"]
                },
                {
                    "id": 2,
                    "user": {
                        "name": "Jane Smith",
                        "email": "[email protected]"
                    },
                    "items": ["item4", "item5"]
                }
            ]
            """;

        URI inputUri = createInputFile(complexJson);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(2, lines.size());

        // Verify nested structures are preserved
        assertThat(lines.get(0), containsString("\"user\""));
        assertThat(lines.get(0), containsString("\"items\""));
        assertThat(lines.get(1), containsString("Jane Smith"));
    }

    /**
     * Test with empty JSON array
     */
    @Test
    void shouldHandleEmptyJsonArray() throws Exception {
        String emptyArray = "[]";

        URI inputUri = createInputFile(emptyArray);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(0, lines.size());
    }

    /**
     * Test with custom charset
     */
    @Test
    void shouldHandleCustomCharset() throws Exception {
        String jsonWithSpecialChars = """
            [
                {"name": "Café", "description": "Café au lait"},
                {"name": "Naïve", "description": "Résumé"}
            ]
            """;

        URI inputUri = createInputFile(jsonWithSpecialChars, StandardCharsets.UTF_8);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .charset(Property.ofValue(StandardCharsets.UTF_8.name()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(2, lines.size());
        assertThat(lines.get(0), containsString("Café"));
        assertThat(lines.get(1), containsString("Naïve"));
    }

    /**
     * Test with dynamic from property (Pebble expression)
     */
    @Test
    void shouldHandleDynamicFromProperty() throws Exception {
        // Given
        String jsonArray = "[{\"id\":1},{\"id\":2}]";
        URI inputUri = createInputFile(jsonArray);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofExpression("{{ fileUri }}"))
            .build();

        // When
        RunContext runContext = runContextFactory.of(java.util.Map.of("fileUri", inputUri.toString()));
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        assertNotNull(output.getUri());
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(2, lines.size());
    }

    /**
     * Test with JSON containing special characters that need escaping
     */
    @Test
    void shouldHandleJsonWithEscapedCharacters() throws Exception {
        String jsonWithEscapes = """
            [
                {"message": "He said \\"Hello\\"", "path": "C:\\\\Users\\\\test"},
                {"message": "Line 1\\nLine 2", "value": "Tab\\there"}
            ]
            """;

        URI inputUri = createInputFile(jsonWithEscapes);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(2, lines.size());

        // Verify escapes are properly handled
        assertThat(lines.get(0), containsString("\\\"Hello\\\""));
        assertThat(lines.get(1), containsString("\\n"));
    }

    /**
     * Test error handling for invalid JSON
     */
    @Test
    void shouldThrowExceptionForInvalidJson() throws Exception {
        String invalidJson = "This is not JSON at all";

        URI inputUri = createInputFile(invalidJson);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When & Then
        RunContext runContext = runContextFactory.of();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    /**
     * Test with empty file
     */
    @Test
    void shouldHandleEmptyFile() throws Exception {
        URI inputUri = createInputFile("");

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        assertNotNull(output.getUri());
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(0, lines.size());
    }

    /**
     * Test real-world API response format
     */
    @Test
    void shouldHandleRealWorldApiResponse() throws Exception {
        String apiResponse = """
            [
               {
                  "id":"1",
                  "name":"Google Pixel 6 Pro",
                  "data":{
                     "color":"Cloudy White",
                     "capacity":"128 GB"
                  }
               },
               {
                  "id":"2",
                  "name":"Apple iPhone 12 Mini, 256GB, Blue",
                  "data":null
               },
               {
                  "id":"3",
                  "name":"Apple iPhone 12 Pro Max",
                  "data":{
                     "color":"Cloudy White",
                     "capacity GB":512
                  }
               }
            ]
            """;

        URI inputUri = createInputFile(apiResponse);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(3, lines.size());

        assertThat(lines.get(0), containsString("Google Pixel 6 Pro"));
        assertThat(lines.get(1), containsString("iPhone 12 Mini"));
        assertThat(lines.get(2), containsString("iPhone 12 Pro Max"));

        // Verify null values are handled
        assertThat(lines.get(1), containsString("null"));
    }

    /**
     * Test with JSONL that has blank lines (should skip them)
     */
    @Test
    void shouldSkipBlankLinesInJsonl() throws Exception {
        String jsonlWithBlanks = """
            {"id":1,"name":"First"}

            {"id":2,"name":"Second"}


            {"id":3,"name":"Third"}
            """;

        URI inputUri = createInputFile(jsonlWithBlanks);

        JsonToJsonl task = JsonToJsonl.builder()
            .from(Property.ofValue(inputUri.toString()))
            .build();

        // When
        RunContext runContext = runContextFactory.of();
        JsonToJsonl.Output output = task.run(runContext);

        // Then
        List<String> lines = readLinesFromUri(output.getUri());
        assertEquals(3, lines.size());
        assertThat(lines.get(0), containsString("\"id\":1"));
        assertThat(lines.get(1), containsString("\"id\":2"));
        assertThat(lines.get(2), containsString("\"id\":3"));
    }

    private URI createInputFile(String content) throws IOException {
        return createInputFile(content, StandardCharsets.UTF_8);
    }

    private URI createInputFile(String content, java.nio.charset.Charset charset) throws IOException {
        File tempFile = File.createTempFile("test-input-", ".json");
        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(tempFile), charset))) {
            writer.write(content);
        }

        return storageInterface.put(
            MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".json"),
            new FileInputStream(tempFile)
        );
    }

    private List<String> readLinesFromUri(URI uri) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(storageInterface.get(MAIN_TENANT, null, uri), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }
}