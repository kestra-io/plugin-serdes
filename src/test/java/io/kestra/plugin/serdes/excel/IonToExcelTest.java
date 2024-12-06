package io.kestra.plugin.serdes.excel;

import bad.robot.excel.matchers.WorkbookMatcher;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
public class IonToExcelTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private void test(String inputIonResourcePath, String expectedExcelResourcePath, boolean header) throws Exception {
        URI inputUri = this.serdesUtils.resourceToStorageObject(SerdesUtils.resourceToFile(inputIonResourcePath));

        IonToExcel writer = IonToExcel.builder()
            .id(IonToExcelTest.class.getSimpleName())
            .type(IonToExcel.class.getName())
            .sheetsTitle(Property.of("Worksheet"))
            .from(inputUri.toString())
            .header(Property.of(header))
            .build();
        IonToExcel.Output excelOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        XSSFWorkbook actual = new XSSFWorkbook(storageInterface.get(null, null, (URI) excelOutput.getUri()));
        XSSFWorkbook expected = new XSSFWorkbook(new FileInputStream(SerdesUtils.resourceToFile(expectedExcelResourcePath)));
        assertThat(actual, WorkbookMatcher.sameWorkbook(expected));
    }

    @Test
    void header() throws Exception {
        this.test("excel/insurance_sample.ion", "excel/insurance_sample.xlsx", true);
    }

    @Test
    void noHeader() throws Exception {
        this.test("excel/insurance_sample.ion", "excel/insurance_sample_no_header.xlsx", false);
    }

    @Test
    void skipRows() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample.xlsx");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.of(source.toString()))
            .skipRows(4)
            .header(Property.of(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        ExcelToIon.Output output = reader.run(runContext);

        assertThat(output.getSize(), is(2L));
    }

    @Test
    void large() throws Exception {
        final Long ROWS_COUNT = 10000L;

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");

        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, "value" + 1);
            map.put("int", 1);
        }

        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            for (int i = 0; i < ROWS_COUNT; i++) {
                FileSerde.write(outputStream, map);
            }
        }

        URI put = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        IonToExcel writer = IonToExcel.builder()
            .id(IonToExcel.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(put.toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        IonToExcel.Output output = writer.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getSize(), is(ROWS_COUNT));

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.of(output.getUri().toString()))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        ExcelToIon.Output outputWriter = reader.run(runContext);

        assertThat(outputWriter.getSize(), is(ROWS_COUNT + 1));
    }

    @Test
    void styles() throws Exception {
        final Long ROWS_COUNT = 10000L;

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");

        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, Instant.now());
        }

        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            for (int i = 0; i < ROWS_COUNT; i++) {
                FileSerde.write(outputStream, map);
            }
        }

        URI put = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        IonToExcel writer = IonToExcel.builder()
            .id(IonToExcel.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(put.toString())
            .styles(Property.of(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        IonToExcel.Output output = writer.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getSize(), is(ROWS_COUNT));

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(Property.of(output.getUri().toString()))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        ExcelToIon.Output outputWriter = reader.run(runContext);

        assertThat(outputWriter.getSize(), is(ROWS_COUNT + 1));
    }

    @Test
    void multiSheets() throws Exception {
        URI inputUri = this.serdesUtils.resourceToStorageObject(
            SerdesUtils.resourceToFile("excel/insurance_sample.ion")
        );

        IonToExcel writer = IonToExcel.builder()
            .id(IonToExcelTest.class.getSimpleName())
            .type(IonToExcel.class.getName())
            .sheetsTitle(Property.of("Worksheet"))
            .from(
                Map.of(
                    "Worksheet_1", inputUri.toString(),
                    "Worksheet_2", inputUri.toString(),
                    "Worksheet_3", inputUri.toString()
                ))
            .build();

        IonToExcel.Output excelOutput = writer.run(
            TestsUtils.mockRunContext(
                runContextFactory,
                writer,
                ImmutableMap.of()
            )
        );

        XSSFWorkbook actual = new XSSFWorkbook(storageInterface.get(null, null, excelOutput.getUri()));
        XSSFWorkbook expected = new XSSFWorkbook(
            new FileInputStream(
                SerdesUtils.resourceToFile("excel/insurance_sample_multiple_sheets.xlsx")
            )
        );
        assertThat(actual, WorkbookMatcher.sameWorkbook(expected));
    }
}
