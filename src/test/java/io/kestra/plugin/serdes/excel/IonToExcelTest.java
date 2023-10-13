package io.kestra.plugin.serdes.excel;

import bad.robot.excel.matchers.WorkbookMatcher;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class IonToExcelTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private void test(String file, boolean header) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(source.toString())
            .header(header)
            .build();
        ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToExcel writer = IonToExcel.builder()
            .id(IonToExcelTest.class.getSimpleName())
            .type(IonToExcel.class.getName())
            .sheetsTitle("Worksheet")
            .from(ionOutput.getUris().get("Worksheet").toString())
            .header(header)
            .build();
        IonToExcel.Output excelOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        XSSFWorkbook actual = new XSSFWorkbook(storageInterface.get(null, excelOutput.getUri()));
        XSSFWorkbook expected = new XSSFWorkbook(new FileInputStream(sourceFile));
        assertThat(actual, WorkbookMatcher.sameWorkbook(expected));
    }

    @Test
    void header() throws Exception {
        this.test("excel/insurance_sample.xlsx", true);
    }

    @Test
    void noHeader() throws Exception {
        this.test("excel/insurance_sample_no_header.xlsx", false);
    }

    @Test
    void skipRows() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample.xlsx");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ExcelToIon reader = ExcelToIon.builder()
            .id(ExcelToIonTest.class.getSimpleName())
            .type(ExcelToIon.class.getName())
            .from(source.toString())
            .skipRows(4)
            .header(false)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        ExcelToIon.Output output = reader.run(runContext);

        assertThat(output.getSize(), is(2));
    }

}
