package io.kestra.plugin.serdes.excel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.github.pjfanning.xlsx.StreamingReader;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.preview.FilePreview;
import io.kestra.core.preview.FileRenderer;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Excel file renderer",
    description = """
        Preview .xlsx files inside the Kestra UI in streaming mode, with low, constant \
        memory usage. Only the first sheet is previewed, the first row is treated as \
        the header, and each following row is rendered as a record mapping header names \
        to values."""
)
@Plugin
public class ExcelFileRenderer implements FileRenderer {
    @Override
    public boolean supports(String extension) {
        return "xlsx".equalsIgnoreCase(extension);
    }

    // No @Override: this branch's kestraVersion predates FileRenderer.extensions()
    // (kestra-io/kestra#16054). Overrides correctly once that dependency updates.
    public Set<String> extensions() {
        return Set.of("xlsx");
    }

    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        List<Object> rows = new ArrayList<>();
        boolean truncated = false;

        try (Workbook workbook = StreamingReader.builder().rowCacheSize(1).open(inputStream)) {
            if (workbook.getNumberOfSheets() == 0) {
                return FilePreview.builder()
                    .content(rows)
                    .truncated(false)
                    .extension(extension)
                    .type(FilePreview.Type.LIST)
                    .build();
            }

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.rowIterator();

            List<String> headers = new ArrayList<>();
            if (rowIterator.hasNext()) {
                Row headerRow = rowIterator.next();
                short lastColNum = headerRow.getLastCellNum();
                for (int i = 0; i < lastColNum; i++) {
                    Cell cell = headerRow.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    headers.add(cell != null ? String.valueOf(extractCellValue(cell)) : "Column" + i);
                }
            }

            while (rowIterator.hasNext() && rows.size() < maxRows) {
                Row row = rowIterator.next();
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    record.put(headers.get(i), cell != null ? extractCellValue(cell) : null);
                }
                rows.add(record);
            }

            truncated = rowIterator.hasNext();
        }

        return FilePreview.builder()
            .content(rows)
            .truncated(truncated)
            .extension(extension)
            .type(FilePreview.Type.LIST)
            .build();
    }

    private Object extractCellValue(Cell cell) {
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case BOOLEAN -> cell.getBooleanCellValue();
            case NUMERIC -> convertNumeric(cell);
            case FORMULA -> extractFormulaValue(cell);
            default -> null;
        };
    }

    private Object extractFormulaValue(Cell cell) {
        return switch (cell.getCachedFormulaResultType()) {
            case NUMERIC -> convertNumeric(cell);
            case STRING -> cell.getRichStringCellValue().getString();
            case BOOLEAN -> cell.getBooleanCellValue();
            default -> null;
        };
    }

    private Object convertNumeric(Cell cell) {
        if (DateUtil.isCellDateFormatted(cell)) {
            var date = cell.getDateCellValue();
            return date != null ? date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString() : null;
        }
        return cell.getNumericCellValue();
    }
}
