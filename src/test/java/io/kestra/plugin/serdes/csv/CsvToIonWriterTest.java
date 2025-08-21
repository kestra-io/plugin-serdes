package io.kestra.plugin.serdes.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import de.siegmar.fastcsv.reader.CsvParseException;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            .from(Property.ofValue(source.toString()))
            .fieldSeparator(Property.ofValue(";".charAt(0)))
            .header(Property.ofValue(header))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToCsv writer = IonToCsv.builder()
            .id(CsvToIonWriterTest.class.getSimpleName())
            .type(IonToCsv.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .fieldSeparator(Property.ofValue(";".charAt(0)))
            .alwaysDelimitText(Property.ofValue(true))
            .lineDelimiter(Property.ofValue((file.equals("csv/insurance_sample.csv") ? "\r\n" : "\n")))
            .header(Property.ofValue(header))
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
            .from(Property.ofValue(source.toString()))
            .fieldSeparator(Property.ofValue(";".charAt(0)))
            .skipRows(Property.ofValue(4))
            .header(Property.ofValue(false))
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

    @Test
    void exceedsBufferThrows() throws Exception {
        int n = 50;
        String csv = "col1\n\"" + "x".repeat(n) + "\"\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/tinybuf.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("exceedsBufferThrows")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .maxBufferSize(Property.ofValue(8))
            .maxFieldSize(Property.ofValue(1024))
            .header(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());

        Throwable cause = assertThrows(CsvParseException.class, () -> reader.run(runContext));
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }

        assertThat(cause.getMessage(), containsString("maximum buffer size"));
    }


    @Test
    void largeQuotedFieldParsesWhenBufferSufficient() throws Exception {
        String csv = "col1\n\"" + "x".repeat(50) + "\"\n";
        URI src = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/okbuf.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        CsvToIon reader = CsvToIon.builder()
            .id("largeQuotedFieldParsesWhenBufferSufficient")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .maxBufferSize(Property.ofValue(128))
            .maxFieldSize(Property.ofValue(1024))
            .header(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        CsvToIon.Output out = reader.run(runContext);
        assertThat(out.getUri(), is(notNullValue()));
    }
}
