package io.kestra.plugin.serdes.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
import jakarta.validation.constraints.NotBlank;
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
    title = "Read data from Excel into a row-wise ION-serialized format"
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
    private Property<String> from;

    @Schema(
        title = "The sheets title to be included"
    )
    private Property<List<String>> sheetsTitle;

    @Schema(
        title = "The name of a supported character set"
    )
    @Builder.Default
    private Property<String> charset = Property.of("UTF-8");

    @Schema(
        title = "Determines how values should be rendered in the output",
        description = "Possible values: FORMATTED_VALUE, UNFORMATTED_VALUE, FORMULA"
    )
    @Builder.Default
    private Property<ValueRender> valueRender = Property.of(ValueRender.UNFORMATTED_VALUE);

    @Schema(
        title = "How dates, times, and durations should be represented in the output",
        description = "Possible values: SERIAL_NUMBER, FORMATTED_STRING"
    )
    @Builder.Default
    private Property<DateTimeRender> dateTimeRender = Property.of(DateTimeRender.UNFORMATTED_VALUE);

    @Schema(
        title = "Whether the first row should be treated as the header"
    )
    @Builder.Default
    private Property<Boolean> header = Property.of(true);

    @Schema(
        title = "Specifies if empty rows should be skipped"
    )
    @Builder.Default
    private Property<Boolean> skipEmptyRows = Property.of(false);

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

            // read values
            Map<String, URI> uris = new HashMap<>();
            AtomicInteger rowsCount = new AtomicInteger();

            AtomicInteger skipped = new AtomicInteger();
            var renderedValueRender = runContext.render(this.valueRender).as(ValueRender.class).orElseThrow();
            var renderedSkipEmptyRowsValue = runContext.render(this.skipEmptyRows).as(Boolean.class).orElseThrow();
            var renderedDateTimeRender = runContext.render(this.dateTimeRender).as(DateTimeRender.class).orElseThrow();
            for (Sheet sheet : selectedSheets) {
                List<Object> values = new ArrayList<>();

                sheet.rowIterator().forEachRemaining(row -> {
                    List<Object> rowValues = new ArrayList<>();
                    if (this.skipRows > 0 && skipped.get() < this.skipRows) {
                        skipped.incrementAndGet();
                        return;
                    }

                    for (int i = row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
                        Cell cell = row.getCell(i);
                        if (cell != null) {
                            if (renderedValueRender.equals(ValueRender.FORMATTED_VALUE)) {
                                extractValue(rowValues, cell, renderedSkipEmptyRowsValue, renderedDateTimeRender);
                            } else if (renderedValueRender.equals(ValueRender.FORMULA)) {
                                switch (cell.getCachedFormulaResultType()) {
                                    case NUMERIC -> rowValues.add(convertNumeric(cell, renderedDateTimeRender));
                                    case STRING -> rowValues.add(cell.getRichStringCellValue().getString());
                                }
                            } else {
                                extractValue(rowValues, cell, renderedSkipEmptyRowsValue, renderedDateTimeRender);
                            }
                        }
                    }
                    values.add(rowValues);
                });

                rowsCount.addAndGet(values.size());

                uris.put(sheet.getSheetName(), convertToIon(runContext, values));
            }

            return Output.builder()
                .uris(uris)
                .size(rowsCount.get())
                .build();
        }
    }

    public void extractValue(List<Object> rowValues, Cell cell, boolean skipEmptyRowsValue, DateTimeRender dateTimeRender) {
        switch (cell.getCellType()) {
            case STRING -> rowValues.add(cell.getStringCellValue());
            case BOOLEAN -> rowValues.add(cell.getBooleanCellValue());
            case NUMERIC -> rowValues.add(convertNumeric(cell, dateTimeRender));
            case FORMULA -> {
                switch (cell.getCachedFormulaResultType()) {
                    case NUMERIC -> rowValues.add(convertNumeric(cell, dateTimeRender));
                    case STRING -> rowValues.add(cell.getRichStringCellValue().getString());
                }
            }
            case BLANK -> {
                if (!skipEmptyRowsValue) {
                    rowValues.add(cell.getStringCellValue());
                }
            }
            default -> {
            }
        }
    }

    private URI convertToIon(RunContext runContext, List<Object> values) throws IOException, IllegalVariableEvaluationException {
        if (runContext.render(header).as(Boolean.class).orElseThrow()) {
            List<Object> headers = (List<Object>) values.remove(0);
            List<Object> convertedSheet = new LinkedList<>();

            for (Object value : values) {
                List<Object> list = (List<Object>) value;
                Map<Object, Object> row = new LinkedHashMap<>();

                for (int j = 0, headerPosition = 0; j < headers.size(); j++) {
                    Object header = headers.get(headerPosition++);
                    row.put(header, list.get(j));
                }

                convertedSheet.add(row);
            }

            return runContext.storage().putFile(this.store(runContext, convertedSheet));
        }

        return runContext.storage().putFile(this.store(runContext, values));
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
