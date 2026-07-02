package io.kestra.plugin.serdes.json;

import io.kestra.core.preview.FilePreview;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonlFileRendererTest {
    @ParameterizedTest
    @CsvSource({ "0, false", "100, false", "101, true" })
    void testTruncatedByLineCount(int lineCount, boolean truncated) throws Exception {
        InputStream is = jsonlInputStream(lineCount);

        JsonlFileRenderer renderer = new JsonlFileRenderer();
        FilePreview rendered = renderer.render("jsonl", is, Optional.empty(), 100);

        assertThat(rendered.isTruncated(), is(truncated));
    }

    @Test
    void testContent() throws Exception {
        InputStream is = jsonlInputStream(2);

        JsonlFileRenderer renderer = new JsonlFileRenderer();
        FilePreview rendered = renderer.render("jsonl", is, Optional.empty(), 100);

        assertThat(rendered.getType(), is(FilePreview.Type.LIST));
        assertThat(rendered.isTruncated(), is(false));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) rendered.getContent();
        assertThat(rows, hasSize(2));
        assertThat(rows.get(0).get("name"), is("name0"));
    }

    @Test
    void testUnsupportedExtensionThrows() {
        JsonlFileRenderer renderer = new JsonlFileRenderer();
        InputStream is = new ByteArrayInputStream(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> renderer.render("json", is, Optional.empty(), 10));
    }

    private InputStream jsonlInputStream(int lineCount) {
        StringBuilder content = new StringBuilder();
        IntStream.range(0, lineCount)
            .forEach(i -> content.append("{\"id\":").append(i).append(",\"name\":\"name").append(i).append("\"}\n"));
        return new ByteArrayInputStream(content.toString().getBytes(StandardCharsets.UTF_8));
    }
}
