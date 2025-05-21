package io.kestra.plugin.serdes.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class CsvToIonWriterTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private void test(String file, boolean header) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(CsvToIonWriterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.of(source.toString()))
            .fieldSeparator(Property.of(";".charAt(0)))
            .header(Property.of(header))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToCsv writer = IonToCsv.builder()
            .id(CsvToIonWriterTest.class.getSimpleName())
            .type(IonToCsv.class.getName())
            .from(Property.of(readerRunOutput.getUri().toString()))
            .fieldSeparator(Property.of(";".charAt(0)))
            .alwaysDelimitText(Property.of(true))
            .lineDelimiter(Property.of((file.equals("csv/insurance_sample.csv") ? "\r\n" : "\n")))
            .header(Property.of(header))
            .build();
        IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri()))),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void header() throws Exception {
        this.test("csv/insurance_sample.csv", true);
    }

    @Test
    void noHeader() throws Exception {
        this.test("csv/insurance_sample_no_header.csv", false);
    }

    @Test
    void skipRows() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/insurance_sample.csv");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(CsvToIonWriterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.of(source.toString()))
            .fieldSeparator(Property.of(";".charAt(0)))
            .skipRows(Property.of(4))
            .header(Property.of(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        reader.run(runContext);

        Counter records = (Counter) runContext.metrics()
            .stream()
            .filter(metricEntry -> metricEntry.getName().equals("records"))
            .findFirst()
            .get();

        assertThat(records.getValue(), is(2D));
    }

}
