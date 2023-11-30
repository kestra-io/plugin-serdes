package io.kestra.plugin.serdes.excel;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@MicronautTest
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
        try(OutputStream output = new FileOutputStream(tempFile)) {

            File sourceFile = SerdesUtils.resourceToFile("excel/insurance_sample.xlsx");
            URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

            ExcelToIon reader = ExcelToIon.builder()
                .id(ExcelToIonTest.class.getSimpleName())
                .type(ExcelToIon.class.getName())
                .from(source.toString())
                .header(true)
                .build();
            ExcelToIon.Output ionOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(null, ionOutput.getUris().get("Worksheet"))));

            assertThat(out, containsString("policyID:\"333743\""));
            assertThat(out, containsString("point_latitude:30.102261"));
        }
    }
}
