package io.kestra.plugin.serdes.excel;

import io.kestra.core.preview.FilePreview;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExcelFileRendererTest {
    @ParameterizedTest
    @CsvSource({ "0, false", "100, false", "101, true" })
    void testTruncatedByRowCount(int rowCount, boolean truncated) throws Exception {
        InputStream is = excelInputStream(rowCount);

        ExcelFileRenderer renderer = new ExcelFileRenderer();
        FilePreview rendered = renderer.render("xlsx", is, Optional.empty(), 100);

        assertThat(rendered.isTruncated(), is(truncated));
    }

    @Test
    void testContent() throws Exception {
        InputStream is = excelInputStream(2);

        ExcelFileRenderer renderer = new ExcelFileRenderer();
        FilePreview rendered = renderer.render("xlsx", is, Optional.empty(), 100);

        assertThat(rendered.getType(), is(FilePreview.Type.LIST));
        assertThat(rendered.isTruncated(), is(false));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) rendered.getContent();
        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst().keySet(), containsInAnyOrder("id", "name"));
        assertThat(rows.getFirst().get("name"), is("name0"));
        assertThat(rows.get(1).get("name"), is("name1"));
    }

    @Test
    void testUnsupportedExtensionThrows() {
        ExcelFileRenderer renderer = new ExcelFileRenderer();
        InputStream is = new ByteArrayInputStream(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> renderer.render("csv", is, Optional.empty(), 10));
    }

    private InputStream excelInputStream(int rowCount) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            var sheet = workbook.createSheet("Sheet1");

            var headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("id");
            headerRow.createCell(1).setCellValue("name");

            for (int i = 0; i < rowCount; i++) {
                var row = sheet.createRow(i + 1);
                row.createCell(0).setCellValue(i);
                row.createCell(1).setCellValue("name" + i);
            }

            workbook.write(output);
        }

        return new ByteArrayInputStream(output.toByteArray());
    }
}
