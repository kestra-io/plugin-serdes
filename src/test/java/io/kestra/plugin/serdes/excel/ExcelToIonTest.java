package io.kestra.plugin.serdes.excel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.amazon.ion.system.IonSystemBuilder;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class ExcelToIonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @ParameterizedTest
    @MethodSource("should_get_a_correct_ion_inputs")
    void should_get_a_correct_ion(String excelFile, String excelSheet, Boolean withHeaders, List<String> expectedStrings) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream unused = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile(excelFile);
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(Property.ofValue(source.toString()))
                .header(Property.ofValue(withHeaders))
                .build();
            ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get(excelSheet)));

            expectedStrings.forEach(expectedString -> assertThat(out, containsString(expectedString)));
        }
    }

    private static Stream<Arguments> should_get_a_correct_ion_inputs() {
        return Stream.of(
            Arguments.of(
                "excel/insurance_sample.xlsx",
                "Worksheet",
                true,
                List.of(
                    "policyID:\"333743\"",
                    "point_latitude:30.102261"
                )
            ),
            Arguments.of(
                "excel/insurance_sample.xlsx",
                "Worksheet",
                false,
                List.of("\"policyID\",\"statecode\"")
            ),
            Arguments.of(
                "excel/missing_cells_sample.xlsx",
                "Sheet1",
                true,
                List.of("abc")
            ),
            Arguments.of(
                "excel/sample_with_a_full_missing_column.xlsx",
                "Sheet1",
                true,
                List.of("PrizeMasterType:null")
            ),
            Arguments.of(
                "excel/sample_with_date.xlsx",
                "Sheet1",
                true,
                List.of("2025-04-01")
            ),
            Arguments.of(
                "excel/sample_with_date.xlsx",
                "Sheet1",
                true,
                List.of("2025-06-27 14h22")
            )
        );
    }

    @Test
    void multiSheets() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream unused = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample_multiple_sheets.xlsx");
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(Property.ofValue(source.toString()))
                .sheetsTitle(
                    Property.ofValue(
                        List.of(
                            "Worksheet_1",
                            "Worksheet_2",
                            "Worksheet_3"
                        )
                    )
                )
                .header(Property.ofValue(true))
                .build();

            ExcelToIon.Output ionOutput = reader.run(
                TestsUtils.mockRunContext(
                    runContextFactory,
                    reader,
                    ImmutableMap.of()
                )
            );

            String outWorkSheet1 = ionToText(
                storageInterface.get(
                    TenantService.MAIN_TENANT,
                    null,
                    ionOutput.getUris().get("Worksheet_1")
                )
            );

            assertThat(outWorkSheet1, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet1, containsString("point_latitude:30.102261"));

            String outWorkSheet2 = ionToText(
                storageInterface.get(
                    TenantService.MAIN_TENANT,
                    null,
                    ionOutput.getUris().get("Worksheet_2")
                )
            );

            assertThat(outWorkSheet2, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet2, containsString("point_latitude:30.102261"));

            String outWorkSheet3 = ionToText(
                storageInterface.get(
                    TenantService.MAIN_TENANT,
                    null,
                    ionOutput.getUris().get("Worksheet_3")
                )
            );

            assertThat(outWorkSheet3, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet3, containsString("point_latitude:30.102261"));
        }
    }

    @Test
    void formattedValue_doesNotThrowOnTextCells() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample.xlsx");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .valueRender(Property.ofValue(ValueRender.FORMATTED_VALUE))
            .build();

        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Worksheet")));

        assertThat(out, containsString("policyID:\"333743\""));
    }

    @ParameterizedTest
    @EnumSource(DateTimeRender.class)
    void formattedValue_rendersTextNumericDateAndBooleanCells(DateTimeRender dateTimeRender) throws Exception {
        var sourceFile = createWorkbookWithMixedTypes();
        var source = this.serdesUtils.resourceToStorageObject(sourceFile);

        var reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .valueRender(Property.ofValue(ValueRender.FORMATTED_VALUE))
            .dateTimeRender(Property.ofValue(dateTimeRender))
            .build();

        var ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        var out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1")));

        assertThat(out, containsString("name:\"alice\""));
        assertThat(out, containsString("active:\"TRUE\""));
        assertThat(out, not(containsString("birthDate:null")));
        // a date-formatted formula cell must honour dateTimeRender exactly like the plain date cell it mirrors
        assertThat(fieldValue(out, "birthDateFormula"), is(fieldValue(out, "birthDate")));
    }

    // creates an in-memory workbook mixing text, numeric, date, boolean and date-formatted formula cells,
    // since none of the committed fixtures contain a boolean cell
    private static File createWorkbookWithMixedTypes() throws IOException {
        var file = File.createTempFile("excel_mixed_types_", ".xlsx");
        file.deleteOnExit();
        try (var workbook = new XSSFWorkbook()) {
            var sheet = workbook.createSheet("Sheet1");

            var header = sheet.createRow(0);
            header.createCell(0).setCellValue("name");
            header.createCell(1).setCellValue("amount");
            header.createCell(2).setCellValue("birthDate");
            header.createCell(3).setCellValue("active");
            header.createCell(4).setCellValue("birthDateFormula");

            var dateStyle = workbook.createCellStyle();
            dateStyle.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd"));

            var row = sheet.createRow(1);
            row.createCell(0).setCellValue("alice");
            row.createCell(1).setCellValue(42.5);
            var dateCell = row.createCell(2, CellType.NUMERIC);
            dateCell.setCellValue(LocalDate.of(2025, 4, 1));
            dateCell.setCellStyle(dateStyle);
            row.createCell(3).setCellValue(true);
            // formula whose cached result is a date-formatted number: getCellType() reports FORMULA, not NUMERIC
            var formulaCell = row.createCell(4, CellType.FORMULA);
            formulaCell.setCellFormula("C2");
            formulaCell.setCellValue(DateUtil.getExcelDate(LocalDate.of(2025, 4, 1)));
            formulaCell.setCellStyle(dateStyle);

            try (var out = new FileOutputStream(file)) {
                workbook.write(out);
            }
        }
        return file;
    }

    // extracts a single field value out of the ION text rendering, e.g. `birthDate:2025-04-01T...`
    private static String fieldValue(String ion, String field) {
        var matcher = Pattern.compile("\\b" + Pattern.quote(field) + ":([^,}]+)").matcher(ion);
        assertThat("field " + field + " not found in " + ion, matcher.find(), is(true));
        return matcher.group(1).trim();
    }

    // ionOutput files are binary ION; load them via the InputStream and re-render to ION text
    // instead of decoding the binary bytes as UTF-8 text, which would corrupt the BVM header.
    private static String ionToText(InputStream input) throws IOException {
        try (input) {
            return IonSystemBuilder.standard().build().getLoader().load(input).toString();
        }
    }

    @Test
    void formulaWithoutCachedValueFallsBackToFormulaString() throws Exception {
        // writers such as openpyxl persist a formula cell with an empty cached <v/> result instead
        // of omitting it; POI still reports that as a numeric result, so reading it must not crash.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Sheet1");

            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("a");
            headerRow.createCell(1).setCellValue("b");
            headerRow.createCell(2).setCellValue("total");

            Row dataRow = sheet.createRow(1);
            dataRow.createCell(0).setCellValue(2);
            dataRow.createCell(1).setCellValue(3);
            dataRow.createCell(2).setCellFormula("A2*B2");

            var bos = new ByteArrayOutputStream();
            workbook.write(bos);
            Files.write(sourceFile.toPath(), withEmptyCachedFormulaResult(bos.toByteArray(), "<f>A2*B2</f>"));
        }

        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();
        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1")));
        assertThat(out, containsString("total:\"A2*B2\""));
    }

    @Test
    void formulaWithoutCachedValueUnderFormattedValueRenderFallsBackToFormulaString() throws Exception {
        // getFormattedValue() hits the very same StreamingCell.getNumericCellValue() call as
        // getFormula(), so the empty cached result crashes this render mode too.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Sheet1");

            Row dataRow = sheet.createRow(0);
            dataRow.createCell(0).setCellValue(2);
            dataRow.createCell(1).setCellValue(3);
            dataRow.createCell(2).setCellFormula("A1*B1");

            var bos = new ByteArrayOutputStream();
            workbook.write(bos);
            Files.write(sourceFile.toPath(), withEmptyCachedFormulaResult(bos.toByteArray(), "<f>A1*B1</f>"));
        }

        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(false))
            .valueRender(Property.ofValue(ValueRender.FORMATTED_VALUE))
            .build();
        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1")));
        assertThat(out, containsString("\"A1*B1\""));
    }

    @Test
    void formulaWithoutCachedValueUnderFormulaRenderFallsBackToFormulaString() throws Exception {
        // isolated to a single formula-only cell: getFormula() also calls getCachedFormulaResultType()
        // unconditionally for FORMULA render mode, which is only valid on an actual formula cell.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Sheet1");

            Row dataRow = sheet.createRow(0);
            dataRow.createCell(0).setCellFormula("2*3");

            var bos = new ByteArrayOutputStream();
            workbook.write(bos);
            Files.write(sourceFile.toPath(), withEmptyCachedFormulaResult(bos.toByteArray(), "<f>2*3</f>"));
        }

        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(false))
            .valueRender(Property.ofValue(ValueRender.FORMULA))
            .build();
        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1")));
        assertThat(out, containsString("\"2*3\""));
    }

    @Test
    void formulaWithCachedValueReturnsEvaluatedResult() throws Exception {
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Sheet1");

            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("a");
            headerRow.createCell(1).setCellValue("b");
            headerRow.createCell(2).setCellValue("total");

            Row dataRow = sheet.createRow(1);
            dataRow.createCell(0).setCellValue(2);
            dataRow.createCell(1).setCellValue(3);
            dataRow.createCell(2).setCellFormula("A2*B2");

            workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();

            try (var out = new FileOutputStream(sourceFile)) {
                workbook.write(out);
            }
        }

        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();
        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        String out = ionToText(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1")));
        assertThat(out, containsString("total:6"));
    }

    @Test
    void malformedNonFormulaNumericCellStillThrows() throws Exception {
        // the NumberFormatException fallback is scoped to formula cells; a plain numeric cell with
        // an empty cached value is a genuinely malformed file and must still surface as an error.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Sheet1");
            sheet.createRow(0).createCell(0).setCellValue(2);

            var bos = new ByteArrayOutputStream();
            workbook.write(bos);
            Files.write(sourceFile.toPath(), rewriteSheetXml(bos.toByteArray(), "<v>2.0</v>", "<v></v>"));
        }

        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(false))
            .build();

        assertThrows(NumberFormatException.class, () -> reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of())));
    }

    // POI never writes a formula cell without evaluating it first, so this rewrites the generated
    // xlsx's sheet XML to inject an empty <v/> after the given formula tag, mimicking what
    // openpyxl (and other writers) produce for an un-evaluated formula.
    private static byte[] withEmptyCachedFormulaResult(byte[] xlsx, String formulaTag) throws IOException {
        return rewriteSheetXml(xlsx, formulaTag + "</c>", formulaTag + "<v></v></c>");
    }

    private static byte[] rewriteSheetXml(byte[] xlsx, String search, String replace) throws IOException {
        var result = new ByteArrayOutputStream();
        try (var zis = new ZipInputStream(new ByteArrayInputStream(xlsx));
             var zos = new ZipOutputStream(result)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                byte[] content = zis.readAllBytes();
                if (entry.getName().equals("xl/worksheets/sheet1.xml")) {
                    content = new String(content, StandardCharsets.UTF_8)
                        .replace(search, replace)
                        .getBytes(StandardCharsets.UTF_8);
                }
                zos.putNextEntry(new ZipEntry(entry.getName()));
                zos.write(content);
                zos.closeEntry();
            }
        }
        return result.toByteArray();
    }
}
