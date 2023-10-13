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
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile(".xlsx").toFile();

        URI from = new URI(runContext.render(this.from));

        this.init(runContext);

        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
            XSSFWorkbook workbook = new XSSFWorkbook();
            FileOutputStream outputStream = new FileOutputStream(tempFile);
            ) {
            XSSFSheet sheet = workbook.createSheet(sheetsTitle);
            Flowable<Object> flowable = Flowable.create(FileSerde.reader(reader), BackpressureStrategy.BUFFER)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;
                    private int rowAt = 0;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void accept(Object row) {
                        if (row instanceof List casted) {
                            XSSFRow xssfRow = sheet.createRow(rowAt++);

                            if (header) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            int cellAt = 0;
                            for (final Object value : casted) {
                                createCell(workbook, sheet, value, xssfRow, cellAt);
                                ++cellAt;
                            }

                        } else if (row instanceof Map casted) {
                            XSSFRow xssfRow = sheet.createRow(rowAt++);

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
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    // Could be replaced with switch case in java21
    @Deprecated(forRemoval = true)
    private void createCell(Workbook workbook, Sheet sheet, Object value, XSSFRow xssfRow, int rowNumber) {
        XSSFCell cell = xssfRow.createCell(rowNumber);

        if (value instanceof Number) {

            if (value instanceof Double doubleValue) {
                cell.setCellValueImpl(doubleValue);
            } else if (value instanceof Integer intValue) {
                cell.setCellValueImpl(intValue);
            } else if (value instanceof Float floatValue) {
                cell.setCellValueImpl(floatValue);
            }

            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof Boolean) {
            cell.setCellType(CellType.BOOLEAN);
        } else if (value instanceof Date date) {


            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof LocalDate date) {
            sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));

            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof LocalDateTime date) {
            sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));

            cell.setCellValue(DateUtil.getExcelDate(date));
            cell.setCellType(CellType.NUMERIC);
        } else if (value instanceof Instant instant) {
            sheet.setDefaultColumnStyle(cell.getColumnIndex(), getCellStyle(workbook));

            if (getTimeZoneId() != null) {
                LocalDate date = LocalDate.ofInstant(instant, ZoneId.of(getTimeZoneId()));
                cell.setCellValue(DateUtil.getExcelDate(date));
            } else {
                Date date = Date.from(instant);
                cell.setCellValue(DateUtil.getExcelDate(date));
            }
            cell.setCellType(CellType.NUMERIC);
        } else {
            cell.setCellValue(String.valueOf(value));
            cell.setCellType(CellType.STRING);
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
    }
}
