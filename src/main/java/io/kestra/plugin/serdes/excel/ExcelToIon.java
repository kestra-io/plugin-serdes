package io.kestra.plugin.serdes.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.ListUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.poi.ss.usermodel.*;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an Excel file into ION."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Convert an Excel file to the Ion format.",
            code = """
                id: excel_to_ion
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/excel/Products.xlsx

                  - id: to_ion
                    type: io.kestra.plugin.serdes.excel.ExcelToIon
                    from: "{{ outputs.http_download.uri }}"
                """
        )
    }
)
public class ExcelToIon extends Task implements RunnableTask<ExcelToIon.Output> {
    @Schema(
        title = "Source file URI"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "The sheets title to be included"
    )
    private Property<List<String>> sheetsTitle;

    @Schema(
        title = "The name of a supported character set"
    )
    @Builder.Default
    private Property<String> charset = Property.ofValue("UTF-8");

    @Schema(
        title = "Determines how values should be rendered in the output",
        description = "Possible values: FORMATTED_VALUE, UNFORMATTED_VALUE, FORMULA"
    )
    @Builder.Default
    private Property<ValueRender> valueRender = Property.ofValue(ValueRender.UNFORMATTED_VALUE);

    @Schema(
        title = "How dates, times, and durations should be represented in the output",
        description = "Possible values: SERIAL_NUMBER, FORMATTED_STRING"
    )
    @Builder.Default
    private Property<DateTimeRender> dateTimeRender = Property.ofValue(DateTimeRender.UNFORMATTED_VALUE);

    @Schema(
        title = "Whether the first row should be treated as the header"
    )
    @Builder.Default
    private Property<Boolean> header = Property.ofValue(true);

    @Schema(
        title = "Specifies if empty rows should be skipped"
    )
    @Builder.Default
    private Property<Boolean> skipEmptyRows = Property.ofValue(false);

    @Schema(
        title = "Number of lines to skip at the start of the file. Useful if a table has a title and explanation in the first few rows"
    )
    @PositiveOrZero
    @PluginProperty
    @Builder.Default
    private int skipRows = 0;

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (Workbook workbook = StreamingReader.builder().rowCacheSize(1).open(runContext.storage().getFile(from))) {

            List<Sheet> sheets = new ArrayList<>();
            workbook.sheetIterator().forEachRemaining(sheets::add);

            List<String> includedSheetsTitle = ListUtils.emptyOnNull(runContext.render(this.sheetsTitle).asList(String.class))
                .stream()
                .map(throwFunction(runContext::render))
                .toList();

            List<Sheet> selectedSheets = sheets.stream()
                .filter(sheet -> includedSheetsTitle.isEmpty() || includedSheetsTitle.contains(sheet.getSheetName()))
                .toList();

            runContext.metric(Counter.of("sheets", sheets.size()));

            Map<String, URI> uris = new HashMap<>();
            AtomicInteger rowsCount = new AtomicInteger();

            var renderedSkipEmptyRowsValue = runContext.render(this.skipEmptyRows).as(Boolean.class).orElseThrow();
            var renderedHeader = runContext.render(this.header).as(Boolean.class).orElseThrow();
            var renderedValueRender = runContext.render(this.valueRender).as(ValueRender.class).orElseThrow();
            var renderedDateTimeRender = runContext.render(this.dateTimeRender).as(DateTimeRender.class).orElseThrow();

            for (Sheet sheet : selectedSheets) {
                var sheetRawData = new ArrayList<>();
                var rowIterator = sheet.rowIterator();

                var headers = new ArrayList<>();
                var firstColNum = 0;
                var lastColNum = -1;

                if (renderedHeader && rowIterator.hasNext()) {
                    var headerRow = rowIterator.next();
                    var skippedHeaderRows = new AtomicInteger();
                    while (skippedHeaderRows.get() < this.skipRows && rowIterator.hasNext()) {
                        headerRow = rowIterator.next();
                        skippedHeaderRows.incrementAndGet();
                    }

                    if (headerRow != null) {
                        firstColNum = headerRow.getFirstCellNum();
                        lastColNum = headerRow.getLastCellNum();
                        for (int i = firstColNum; i < lastColNum; i++) {
                            var cell = headerRow.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                            if (cell != null) {
                                headers.add(extractCellValue(cell, renderedValueRender, renderedDateTimeRender));
                            } else {
                                headers.add("Column" + i);
                            }
                        }
                    }
                }

                if (lastColNum == -1 && rowIterator.hasNext()) {
                    var firstDataRow = rowIterator.next();
                    lastColNum = firstDataRow.getLastCellNum();
                    if (lastColNum == -1) lastColNum = 0;
                    sheetRawData.add(processRow(firstDataRow, firstColNum, lastColNum, renderedValueRender, renderedSkipEmptyRowsValue, renderedDateTimeRender));
                }

                var skippedDataRows = new AtomicInteger();
                while (rowIterator.hasNext()) {
                    var row = rowIterator.next();
                    if (!renderedHeader && this.skipRows > 0 && skippedDataRows.get() < this.skipRows) {
                        skippedDataRows.incrementAndGet();
                        continue;
                    }
                    sheetRawData.add(processRow(row, firstColNum, lastColNum, renderedValueRender, renderedSkipEmptyRowsValue, renderedDateTimeRender));
                }

                rowsCount.addAndGet(sheetRawData.size());

                if (renderedHeader && !headers.isEmpty()) {
                    var convertedSheet = getConvertedSheet(sheetRawData, headers);
                    uris.put(sheet.getSheetName(), runContext.storage().putFile(this.store(runContext, convertedSheet)));
                } else {
                    uris.put(sheet.getSheetName(), runContext.storage().putFile(this.store(runContext, sheetRawData)));
                }
            }

            return Output.builder()
                .uris(uris)
                .size(rowsCount.get())
                .build();
        }
    }

    private List<Object> getConvertedSheet(ArrayList<Object> sheetRawData, ArrayList<Object> headers) {
        var convertedSheet = new ArrayList<>();
        for (Object rowObj : sheetRawData) {
            List<Object> list = (List<Object>) rowObj;
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int headerIndex = 0; headerIndex < headers.size(); headerIndex++) {
                String headerValue = String.valueOf(headers.get(headerIndex));
                Object cellValue = (headerIndex < list.size() && list.get(headerIndex) != null) ? list.get(headerIndex) : null;
                rowMap.put(headerValue, cellValue);
            }
            convertedSheet.add(rowMap);
        }
        return convertedSheet;
    }

    private List<Object> processRow(Row row, int firstCol, int lastCol, ValueRender valueRender, boolean skipEmptyRowsValue, DateTimeRender dateTimeRender) {
        List<Object> rowValues = new ArrayList<>();
        for (int col = firstCol; col < lastCol; col++) {
            var cell = row.getCell(col, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            if (cell == null || (cell.getCellType() == CellType.BLANK && skipEmptyRowsValue)) {
                rowValues.add(null);
            } else {
                rowValues.add(extractCellValue(cell, valueRender, dateTimeRender));
            }
        }
        return rowValues;
    }

    private Object extractCellValue(Cell cell, ValueRender valueRender, DateTimeRender dateTimeRender) {
        if (valueRender.equals(ValueRender.FORMATTED_VALUE)) {
            return getFormattedValue(cell, dateTimeRender);
        } else if (valueRender.equals(ValueRender.FORMULA)) {
            return getFormula(cell, dateTimeRender);
        } else {
            return getUnformattedValue(cell, dateTimeRender);
        }
    }

    private Object getFormula(Cell cell, DateTimeRender dateTimeRender) {
        return switch (cell.getCachedFormulaResultType()) {
            case NUMERIC -> convertNumeric(cell, dateTimeRender);
            case STRING -> cell.getRichStringCellValue().getString();
            case BOOLEAN -> cell.getBooleanCellValue();
            default -> null; // including ERROR, BLANK, etc
        };
    }

    private Object getFormattedValue(Cell cell, DateTimeRender dateTimeRender) {
        DataFormatter dataFormatter = new DataFormatter();
        if (DateUtil.isCellDateFormatted(cell)) {
            return switch (dateTimeRender) {
                case SERIAL_NUMBER -> cell.getNumericCellValue();
                case FORMATTED_STRING -> dataFormatter.formatCellValue(cell);
                default -> cell.getDateCellValue();
            };
        }
        return dataFormatter.formatCellValue(cell);
    }

    private Object getUnformattedValue(Cell cell, DateTimeRender dateTimeRender) {
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case BOOLEAN -> cell.getBooleanCellValue();
            case NUMERIC -> convertNumeric(cell, dateTimeRender);
            case FORMULA -> getFormula(cell, dateTimeRender);
            default -> null; // including ERROR, BLANK, etc
        };
    }

    private Object convertNumeric(Cell cell, DateTimeRender dateTimeRender) {
        if (DateUtil.isCellDateFormatted(cell)) {
            return switch (dateTimeRender) {
                case SERIAL_NUMBER -> cell.getNumericCellValue();
                case FORMATTED_STRING -> {
                    DataFormatter dataFormatter = new DataFormatter();
                    yield dataFormatter.formatCellValue(cell);
                }
                default -> cell.getDateCellValue();
            };
        }
        return cell.getNumericCellValue();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URIs of files serialized in ION format from specific sheets",
            description = "Because this task can simultaneously read data from multiple sheets, " +
                "it will parse them in the key-value pair format: sheet_name: file_uri. " +
                "Therefore, to access data from Sheet1, use the output syntax: \"{{ outputs.task_id.uris.Sheet1 }}\""
        )
        private Map<String, URI> uris;

        @Schema(
            title = "The number of fetched rows"
        )
        private long size;
    }

    private File store(RunContext runContext, Collection<Object> values) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(values);
            FileSerde.writeAll(output, flux).block();
        }
        return tempFile;
    }
}
