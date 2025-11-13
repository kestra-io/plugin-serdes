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
import io.kestra.plugin.serdes.OnBadLines;
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
            .onBadLines(Property.ofValue(OnBadLines.ERROR))
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

    @Test
    void badLinesErrorThrows() throws Exception {
        String csv = "header1,header2\nvalue1,value2\nvalue3,value4,value5\nvalue6,value7"; // Bad line: value3,value4,value5
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/badLinesError.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("badLinesErrorThrows")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.ERROR))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());

        Throwable thrown = assertThrows(RuntimeException.class, () -> reader.run(runContext));

        assertThat(thrown.getMessage(), containsString("Bad line encountered (field count mismatch): Expected 2, got 3 fields."));
    }
    @Test
    void badLinesWarnAndSkip() throws Exception {
        String csv = "header1,header2\nvalue1,value2\nvalue3,\"value4\nvalue6,value7\nvalue8,value9,value10\nvalue11,value12"; // Bad lines: value3,"value4 (unclosed quote), value8,value9,value10 (field count mismatch)
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/badLinesWarnSkip.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        // Test WARN
        CsvToIon readerWarn = CsvToIon.builder()
            .id("badLinesWarn")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContextWarn = TestsUtils.mockRunContext(runContextFactory, readerWarn, ImmutableMap.of());
        readerWarn.run(runContextWarn);

        Counter recordsWarn = (Counter) runContextWarn.metrics()
            .stream()
            .filter(metricEntry -> metricEntry.getName().equals("records"))
            .findFirst()
            .get();

        assertThat(recordsWarn.getValue(), is(2D)); // header + 3 good lines processed, 2 bad lines skipped

        // Test SKIP
        CsvToIon readerSkip = CsvToIon.builder()
            .id("badLinesSkip")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        RunContext runContextSkip = TestsUtils.mockRunContext(runContextFactory, readerSkip, ImmutableMap.of());
        readerSkip.run(runContextSkip);

        Counter recordsSkip = (Counter) runContextSkip.metrics()
            .stream()
            .filter(metricEntry -> metricEntry.getName().equals("records"))
            .findFirst()
            .get();

        assertThat(recordsSkip.getValue(), is(2D)); // header + 3 good lines processed, 2 bad lines skipped
    }
    @Test
    void testCsvWithBadRows() throws Exception {
        String csv = "name,age,city\n" +
            "Alice,New York\n" +           // less column → bad row
            "Bob,25,London,extra\n" +     // extra column → bad row
            "Charlie,35,Paris";           // correct row

        // Put CSV in storage
        URI src = storageInterface.put(
                TenantService.MAIN_TENANT, null, URI.create("/badRows.csv"),
                new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
                );

        // WARN mode
        CsvToIon readerWarn = CsvToIon.builder()
            .id("badRowsTestWarn")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContextWarn = TestsUtils.mockRunContext(runContextFactory, readerWarn, new java.util.HashMap<String, Object>());
        readerWarn.run(runContextWarn);

        Counter recordsWarn = (Counter) runContextWarn.metrics()
            .stream()
            .filter(metric -> metric.getName().equals("records"))
            .findFirst()
            .orElseThrow();
        assertThat(recordsWarn.getValue(), is(1D));

        // SKIP mode
        CsvToIon readerSkip = CsvToIon.builder()
            .id("badRowsTestSkip")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        RunContext runContextSkip = TestsUtils.mockRunContext(runContextFactory, readerSkip, new java.util.HashMap<String, Object>());
        readerSkip.run(runContextSkip);

        Counter recordsSkip = (Counter) runContextSkip.metrics()
            .stream()
            .filter(metric -> metric.getName().equals("records"))
            .findFirst()
            .orElseThrow();
        assertThat(recordsSkip.getValue(), is(1D));

        // ERROR mode
        CsvToIon readerError = CsvToIon.builder()
            .id("badRowsTestError")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.ERROR))
            .build();

        RunContext runContextError = TestsUtils.mockRunContext(runContextFactory, readerError, new java.util.HashMap<String, Object>());
        Throwable thrown = assertThrows(RuntimeException.class, () -> readerError.run(runContextError));
        assertThat(thrown.getMessage(), containsString("Bad line encountered"));
    }
}