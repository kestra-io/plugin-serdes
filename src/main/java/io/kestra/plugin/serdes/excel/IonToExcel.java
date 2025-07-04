package io.kestra.plugin.serdes.excel;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.AbstractTextWriter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert an ION file into Excel."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Download a CSV file and convert it to the Excel file format.",
            code = """
                id: ion_to_excel
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: convert
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.http_download.uri }}"

                  - id: to_excel
                    type: io.kestra.plugin.serdes.excel.IonToExcel
                    from: "{{ outputs.convert.uri }}"
                """
        ),
        @Example(
            full = true,
            title = "Download CSV files and convert them into an Excel file with dedicated sheets.",
            code = """
                id: excel
                namespace: company.team

                tasks:
                  - id: dataset1
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: dataset2
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/fruit.csv

                  - id: convert1
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.dataset1.uri }}"

                  - id: convert2
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.dataset2.uri }}"

                  - id: write
                    type: io.kestra.plugin.serdes.excel.IonToExcel
                    from:
                      Sheet_1: "{{ outputs.convert1.uri }}"
                      Sheet_2: "{{ outputs.convert2.uri }}"
                """
        )
    }
)
public class IonToExcel extends AbstractTextWriter implements RunnableTask<IonToExcel.Output> {

    @NotNull
    @Schema(
        title = "Source file URI",
        anyOf = {String.class, Map.class}
    )
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "The name of a supported character set",
        defaultValue = "UTF-8"
    )
    @Builder.Default
    private Property<String> charset = Property.ofValue("UTF-8");

    @Schema(
        title = "The sheet title to be used when writing data to an Excel spreadsheet",
        defaultValue = "Sheet"
    )
    @Builder.Default
    private Property<String> sheetsTitle = Property.ofValue("Sheet");

    @Schema(
        title = "Whether header should be written as the first line"
    )
    @Builder.Default
    private Property<Boolean> header = Property.ofValue(true);

    @Schema(
        title = "Whether styles should be applied to format values",
        description = "Excel is limited to 64000 styles per document, and styles are applied on every date, " +
            "removed this options when you have a lots of values."
    )
    @Builder.Default
    private Property<Boolean> styles = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Long lineCount;

        if (this.from instanceof String fromStr) {
            this.init(runContext);
            try (SXSSFWorkbook workbook = new SXSSFWorkbook(1)) {
                File tempFile = runContext.workingDir().createTempFile(".xlsx").toFile();

                lineCount = writeQuery(runContext, runContext.render(sheetsTitle).as(String.class).orElseThrow(), runContext.render(fromStr), tempFile, workbook);

                return Output.builder()
                    .uri(runContext.storage().putFile(tempFile))
                    .size(lineCount)
                    .build();
            }
        } else if (from instanceof Map<?, ?> fromMap) {
            try (SXSSFWorkbook workbook = new SXSSFWorkbook(1)) {
                File tempFile = runContext.workingDir().createTempFile(".xlsx").toFile();

                lineCount = runContext.renderMap((Map<String, String>) fromMap).entrySet()
                    .stream()
                    .map(throwFunction(entry -> writeQuery(runContext, entry.getKey(), entry.getValue(), tempFile, workbook)))
                    .mapToLong(Long::longValue)
                    .sum();

                return Output.builder()
                    .uri(runContext.storage().putFile(tempFile))
                    .size(lineCount)
                    .build();

            }
        }

        throw new IllegalStateException("Invalid variable: 'from'");
    }

    private Long writeQuery(RunContext runContext, String title, String from, File tempFile, SXSSFWorkbook workbook) throws Exception {
        URI fromUri = new URI(from);
        Long lineCount;

        try (
            Reader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(fromUri), Charset.forName(runContext.render(this.charset).as(String.class).orElseThrow())), FileSerde.BUFFER_SIZE);
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var renderedHeaderValue = runContext.render(this.header).as(Boolean.class).orElseThrow();
            var renderedStyles = runContext.render(this.styles).as(Boolean.class).orElseThrow();
            var renderedTimeZoneId = runContext.render(this.getTimeZoneId()).as(String.class).orElse(null);

            SXSSFSheet sheet = workbook.createSheet(title);
            Flux<Object> flowable = FileSerde.readAll(reader)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;
                    private int rowAt = 0;

                    @SuppressWarnings("rawtypes")
                    @Override
                    public void accept(Object row) {
                        if (row instanceof List casted) {
                            SXSSFRow xssfRow = sheet.createRow(rowAt++);

                            if (renderedHeaderValue) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            int cellAt = 0;
                            for (final Object value : casted) {
                                createCell(workbook, sheet, value, xssfRow, cellAt, renderedStyles, renderedTimeZoneId);
                                ++cellAt;
                            }

                        } else if (row instanceof Map casted) {
                            SXSSFRow xssfRow = sheet.createRow(rowAt++);

                            if (!first) {
                                this.first = true;
                                if (renderedHeaderValue) {
                                    int cellAt = 0;
                                    for (final Object value : casted.keySet()) {
                                        createCell(workbook, sheet, value, xssfRow, cellAt++, renderedStyles, renderedTimeZoneId);
                                    }
                                    xssfRow = sheet.createRow(rowAt++);
                                }
                            }

                            int cellAt = 0;
                            for (final Object value : casted.values()) {
                                createCell(workbook, sheet, value, xssfRow, cellAt++, renderedStyles, renderedTimeZoneId);
                            }
                        }
                    }
                });

            // metrics & finalize
            Mono<Long> count = flowable.count();
            lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));

            workbook.write(outputStream);
        }

        return lineCount;
    }

    private void createCell(Workbook workbook, Sheet sheet, Object value, SXSSFRow xssfRow, int rowNumber, boolean styles, String renderedTimeZoneId) {
        SXSSFCell cell = xssfRow.createCell(rowNumber);

        switch (value) {
            case Number number -> {
                if (value instanceof Double doubleValue) {
                    cell.setCellValueImpl(doubleValue);
                } else if (value instanceof Integer intValue) {
                    cell.setCellValueImpl(intValue);
                } else if (value instanceof Float floatValue) {
                    cell.setCellValueImpl(floatValue);
                } else if (value instanceof Long longValue) {
                    cell.setCellValueImpl(longValue);
                } else if (value instanceof BigDecimal bigDecimalValue) {
                    cell.setCellValueImpl(bigDecimalValue.doubleValue());
                } else if (value instanceof BigInteger bigIntegerValue) {
                    cell.setCellValue(bigIntegerValue.toString());
                    cell.setCellType(CellType.STRING);
                    return;
                }

                cell.setCellType(CellType.NUMERIC);
            }
            case Boolean b -> cell.setCellType(CellType.BOOLEAN);
            case Date date -> {
                cell.setCellValue(DateUtil.getExcelDate(date));
                cell.setCellType(CellType.NUMERIC);
            }
            case LocalDate date -> {
                if (styles) {
                    sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
                }

                cell.setCellValue(DateUtil.getExcelDate(date));
                cell.setCellType(CellType.NUMERIC);
            }
            case LocalDateTime date -> {
                if (styles) {
                    sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
                }

                cell.setCellValue(DateUtil.getExcelDate(date));
                cell.setCellType(CellType.NUMERIC);
            }
            case Instant instant -> {
                if (styles) {
                    sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
                }

                if (renderedTimeZoneId != null) {
                    LocalDate date = LocalDate.ofInstant(instant, ZoneId.of(renderedTimeZoneId));
                    cell.setCellValue(DateUtil.getExcelDate(date));
                } else {
                    Date date = Date.from(instant);
                    cell.setCellValue(DateUtil.getExcelDate(date));
                }
                cell.setCellType(CellType.NUMERIC);
            }
            case null, default -> {
                String valueStr = String.valueOf(value);
                if (valueStr.startsWith("Formula:")) {
                    cell.setCellFormula(valueStr.substring(8));
                    cell.setCellType(CellType.FORMULA);
                } else {
                    cell.setCellValue(valueStr);
                    cell.setCellType(CellType.STRING);
                }
            }
        }
    }

    private CellStyle getCellStyle(Workbook workbook) {
        CellStyle cellStyle = workbook.createCellStyle();
        DataFormat dataFormat = workbook.createDataFormat();
        cellStyle.setDataFormat(dataFormat.getFormat(getDateFormat()));
        return cellStyle;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Internal storage URI of the Excel file"
        )
        private URI uri;

        @Schema(
            title = "The number of fetched rows"
        )
        private long size;
    }
}
