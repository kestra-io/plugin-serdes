package io.kestra.plugin.serdes.excel;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.AbstractTextWriter;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import jakarta.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ION-serialized file and transform it to an Excel file"
)
public class IonToExcel extends AbstractTextWriter implements RunnableTask<IonToExcel.Output> {

    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The name of a supported character set",
        defaultValue = "UTF-8"
    )
    @Builder.Default
    @PluginProperty
    private String charset = "UTF-8";

    @Schema(
        title = "The sheet title to be used when writing data to an Excel spreadsheet",
        defaultValue = "Sheet"
    )
    @Builder.Default
    @PluginProperty
    private String sheetsTitle = "Sheet";

    @Schema(
        title = "Whether header should be written as the first line"
    )
    @Builder.Default
    @PluginProperty
    private boolean header = true;

    @Schema(
        title = "Whether styles should be applied to format values",
        description = "Excel is limited to 64000 styles per document, and styles are applied on every date, " +
            "removed this options when you have a lots of values."
    )
    @Builder.Default
    @PluginProperty
    private boolean styles = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile(".xlsx").toFile();

        URI from = new URI(runContext.render(this.from));

        this.init(runContext);

        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
            SXSSFWorkbook workbook = new SXSSFWorkbook(1);
            FileOutputStream outputStream = new FileOutputStream(tempFile)
        ) {
            SXSSFSheet sheet = workbook.createSheet(sheetsTitle);
            Flowable<Object> flowable = Flowable.create(FileSerde.reader(reader), BackpressureStrategy.BUFFER)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;
                    private int rowAt = 0;

                    @SuppressWarnings("rawtypes")
                    @Override
                    public void accept(Object row) {
                        if (row instanceof List casted) {
                            SXSSFRow xssfRow = sheet.createRow(rowAt++);

                            if (header) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            int cellAt = 0;
                            for (final Object value : casted) {
                                createCell(workbook, sheet, value, xssfRow, cellAt);
                                ++cellAt;
                            }

                        } else if (row instanceof Map casted) {
                            SXSSFRow xssfRow = sheet.createRow(rowAt++);

                            if (!first) {
                                this.first = true;
                                if (header) {
                                    int cellAt = 0;
                                    for (final Object value : casted.keySet()) {
                                        createCell(workbook, sheet, value, xssfRow, cellAt++);
                                    }
                                    xssfRow = sheet.createRow(rowAt++);
                                }
                            }

                            int cellAt = 0;
                            for (final Object value : casted.values()) {
                                createCell(workbook, sheet, value, xssfRow, cellAt++);
                            }
                        }
                    }
                });

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();
            runContext.metric(Counter.of("records", lineCount));

            workbook.write(outputStream);


            return Output
                .builder()
                .uri(runContext.putTempFile(tempFile))
                .size(lineCount)
                .build();
        }
    }

    // Could be replaced with switch case in java21
    @Deprecated(forRemoval = true)
    private void createCell(Workbook workbook, Sheet sheet, Object value, SXSSFRow xssfRow, int rowNumber) {
        SXSSFCell cell = xssfRow.createCell(rowNumber);

        if (value instanceof Number) {
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
        } else if (value instanceof Boolean) {
            cell.setCellType(CellType.BOOLEAN);
        } else if (value instanceof Date date) {
            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof LocalDate date) {
            if (this.styles) {
                sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
            }

            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof LocalDateTime date) {
            if (this.styles) {
                sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
            }

            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof Instant instant) {
            if (this.styles) {
                sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));
            }

            if (getTimeZoneId() != null) {
                LocalDate date = LocalDate.ofInstant(instant, ZoneId.of(getTimeZoneId()));
                cell.setCellValue(DateUtil.getExcelDate(date));
            } else {
                Date date = Date.from(instant);
                cell.setCellValue(DateUtil.getExcelDate(date));
            }
            cell.setCellType(CellType.NUMERIC);
        } else {
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
