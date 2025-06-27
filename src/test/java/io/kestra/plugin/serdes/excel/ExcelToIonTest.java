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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

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

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, ionOutput.getUris().get(excelSheet))));

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
}
