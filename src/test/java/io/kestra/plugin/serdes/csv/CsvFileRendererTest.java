package io.kestra.plugin.serdes.csv;

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

class CsvFileRendererTest {
    @ParameterizedTest
    @CsvSource({ "0, false", "100, false", "101, true" })
    void testTruncatedByRowCount(int rowCount, boolean truncated) throws Exception {
        InputStream is = csvInputStream(rowCount);

        CsvFileRenderer renderer = new CsvFileRenderer();
        FilePreview rendered = renderer.render("csv", is, Optional.empty(), 100);

        assertThat(rendered.isTruncated(), is(truncated));
    }

    @Test
    void testContent() throws Exception {
        String content = "id,name\n1,Widget A\n2,Widget B\n";
        InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        CsvFileRenderer renderer = new CsvFileRenderer();
        FilePreview rendered = renderer.render("csv", is, Optional.empty(), 100);

        assertThat(rendered.getType(), is(FilePreview.Type.LIST));
        assertThat(rendered.isTruncated(), is(false));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) rendered.getContent();
        assertThat(rows, hasSize(2));
        assertThat(rows.get(0).get("name"), is("Widget A"));
        assertThat(rows.get(1).get("id"), is("2"));
    }

    @Test
    void testUnsupportedExtensionThrows() {
        CsvFileRenderer renderer = new CsvFileRenderer();
        InputStream is = new ByteArrayInputStream(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> renderer.render("json", is, Optional.empty(), 10));
    }

    private InputStream csvInputStream(int rowCount) {
        StringBuilder content = new StringBuilder("id,name\n");
        IntStream.range(0, rowCount).forEach(i -> content.append(i).append(",name").append(i).append('\n'));
        return new ByteArrayInputStream(content.toString().getBytes(StandardCharsets.UTF_8));
    }
}
