package io.kestra.plugin.serdes.excel;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
public class ExcelToIonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    void ion() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample.xlsx");
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(Property.ofValue(source.toString()))
                .header(Property.ofValue(true))
                .build();
            ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Worksheet"))));

            assertThat(out, containsString("policyID:\"333743\""));
            assertThat(out, containsString("point_latitude:30.102261"));
        }
    }

    @Test
    void multiSheets() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample_multiple_sheets.xlsx");
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(Property.ofValue(source.toString()))
                .sheetsTitle(Property.ofValue(
                    List.of(
                        "Worksheet_1",
                        "Worksheet_2",
                        "Worksheet_3"
                    )
                ))
                .header(Property.ofValue(true))
                .build();

            ExcelToIon.Output ionOutput = reader.run(
                TestsUtils.mockRunContext(
                    runContextFactory,
                    reader,
                    ImmutableMap.of()
                )
            );

            String outWorkSheet1 = CharStreams.toString(
                new InputStreamReader(
                    storageInterface.get(
                        TenantService.MAIN_TENANT,
                        null,
                        ionOutput.getUris().get("Worksheet_1")
                    )
                )
            );

            assertThat(outWorkSheet1, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet1, containsString("point_latitude:30.102261"));

            String outWorkSheet2 = CharStreams.toString(
                new InputStreamReader(
                    storageInterface.get(
                        TenantService.MAIN_TENANT,
                        null,
                        ionOutput.getUris().get("Worksheet_2")
                    )
                )
            );

            assertThat(outWorkSheet2, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet2, containsString("point_latitude:30.102261"));

            String outWorkSheet3 = CharStreams.toString(
                new InputStreamReader(
                    storageInterface.get(
                        TenantService.MAIN_TENANT,
                        null,
                        ionOutput.getUris().get("Worksheet_3")
                    )
                )
            );

            assertThat(outWorkSheet3, containsString("policyID:\"333743\""));
            assertThat(outWorkSheet3, containsString("point_latitude:30.102261"));
        }
    }

    @Test
    void ion_with_missing_cells() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream unused = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile("excel/missing_cells_sample.xlsx");
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(Property.ofValue(source.toString()))
                .header(Property.ofValue(true))
                .build();
            ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get("Sheet1"))));

            assertThat(out, containsString("abc"));
        }
    }
}
