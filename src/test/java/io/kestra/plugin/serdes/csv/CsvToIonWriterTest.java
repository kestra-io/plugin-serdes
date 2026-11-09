package io.kestra.plugin.serdes.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.OnBadLines;
import io.kestra.plugin.serdes.OnEmptyHeader;
import io.kestra.plugin.serdes.SerdesUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import de.siegmar.fastcsv.reader.CsvParseException;
import jakarta.inject.Inject;

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
        assertThat(readerRunOutput.getSize(), is(greaterThan(0L)));
        assertThat(writerRunOutput.getSize(), is(greaterThan(0L)));
        assertThat(readerRunOutput.getSize(), is(writerRunOutput.getSize()));
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
    void noHeaderSingleColumnProducesListRows() throws Exception {
        String csv = "ABC\nXYZ\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/singleColumnNoHeader.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("noHeaderSingleColumnProducesListRows")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(false))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, is(notNullValue()));
        assertThat(rows, hasSize(2));
        assertThat(rows.get(0), is((Object) List.of("ABC")));
        assertThat(rows.get(1), is((Object) List.of("XYZ")));
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
    void skipRowsWithHeader() throws Exception {
        String csv = "junkA,junkB\nh1,h2\n1,2\n3,4\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/skipRowsWithHeader.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("skipRowsWithHeader")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .skipRows(Property.ofValue(1))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(2));
        assertThat(rows.get(0), is((Object) Map.of("h1", "1", "h2", "2")));
        assertThat(rows.get(1), is((Object) Map.of("h1", "3", "h2", "4")));
    }

    @Test
    void skipRowsWithHeaderAndDifferentColumnCount() throws Exception {
        String csv = "onlyOneColumn\nh1,h2\n1,2\n3,4\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/skipRowsWithHeaderAndDifferentColumnCount.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("skipRowsWithHeaderAndDifferentColumnCount")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .skipRows(Property.ofValue(1))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(2));
        assertThat(rows.get(0), is((Object) Map.of("h1", "1", "h2", "2")));
        assertThat(rows.get(1), is((Object) Map.of("h1", "3", "h2", "4")));
    }

    @Test
    void skipRowsBeyondFileEnd() throws Exception {
        String csv = "h1,h2\n1,2\n3,4\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/skipRowsBeyondFileEnd.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("skipRowsBeyondFileEnd")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .skipRows(Property.ofValue(10))
            .header(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        CsvToIon.Output out = reader.run(runContext);

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, is(empty()));
        assertThat(out.getSize(), is(0L));
    }

    @Test
    void negativeSkipRowsIsTreatedAsZero() throws Exception {
        String csv = "h1,h2\n1,2\n3,4\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/negativeSkipRowsIsTreatedAsZero.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("negativeSkipRowsIsTreatedAsZero")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .skipRows(Property.ofValue(-3))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(2));
        assertThat(rows.get(0), is((Object) Map.of("h1", "1", "h2", "2")));
        assertThat(rows.get(1), is((Object) Map.of("h1", "3", "h2", "4")));
    }

    @Test
    void badRowInsideSkipWindowCountsAgainstSkipRows() throws Exception {
        // the first row is unparseable (quoted field larger than the buffer) and sits inside the skip window
        String csv = "\"" + "x".repeat(50) + "\"\nh1,h2\n1,2\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/badRowInsideSkipWindow.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("badRowInsideSkipWindowCountsAgainstSkipRows")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .maxBufferSize(Property.ofValue(8))
            .skipRows(Property.ofValue(1))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        // the dropped bad row consumes the skip budget, so the next line is still read as the header
        assertThat(rows, hasSize(1));
        assertThat(rows.getFirst(), is((Object) Map.of("h1", "1", "h2", "2")));
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
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/okbuf.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

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
    void maxRecordSizeDefaultsToMaxFieldSizeWhenFieldLimitIsAboveDefault() throws Exception {
        String csv = "col1\nvalue1\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/largeRecordLimit.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("maxRecordSizeDefaultsToMaxFieldSizeWhenFieldLimitIsAboveDefault")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .maxBufferSize(Property.ofValue(Integer.MAX_VALUE))
            .maxFieldSize(Property.ofValue(1_073_741_824))
            .header(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        CsvToIon.Output out = reader.run(runContext);

        assertThat(out.getUri(), is(notNullValue()));
        assertThat(out.getSize(), is(1L));
    }

    @Test
    void maxRecordSizeBelowMaxFieldSizeFailsWithClearError() throws Exception {
        String csv = "col1\nvalue1\n";
        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/invalidRecordLimit.csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("maxRecordSizeBelowMaxFieldSizeFailsWithClearError")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .maxFieldSize(Property.ofValue(1024))
            .maxRecordSize(Property.ofValue(512))
            .header(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> reader.run(runContext));

        assertThat(
            thrown.getMessage(),
            is("maxRecordSize (512) must be greater than or equal to maxFieldSize (1024).")
        );
    }

    @Test
    void utf8BomInHeaderIsStripped() throws Exception {
        String csvBody = "code_insee;nom_commune\n1001;L'Abergement-Clémenciat\n";
        byte[] utf8Bom = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

        ByteArrayOutputStream withBom = new ByteArrayOutputStream();
        withBom.write(utf8Bom);
        withBom.write(csvBody.getBytes(StandardCharsets.UTF_8));

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/bomHeader.csv"),
            new ByteArrayInputStream(withBom.toByteArray())
        );

        CsvToIon reader = CsvToIon.builder()
            .id("utf8BomInHeaderIsStripped")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), hasItem("code_insee"));
        assertThat(row.get("code_insee"), is("1001"));
    }

    @Test
    void bomBytesAreNotStrippedForNonUtf8Charset() throws Exception {
        // EF BB BF is the UTF-8 BOM, but under ISO-8859-1 these are three real characters that
        // must be preserved, not stripped, so BOM detection is gated on the configured charset.
        String csvBody = "code_insee;nom_commune\n1001;Abergement\n";
        byte[] utf8Bom = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

        ByteArrayOutputStream withBom = new ByteArrayOutputStream();
        withBom.write(utf8Bom);
        withBom.write(csvBody.getBytes(StandardCharsets.ISO_8859_1));

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/bomNonUtf8.csv"),
            new ByteArrayInputStream(withBom.toByteArray())
        );

        CsvToIon reader = CsvToIon.builder()
            .id("bomBytesAreNotStrippedForNonUtf8Charset")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .charset(Property.ofValue("ISO-8859-1"))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), not(hasItem("code_insee")));
        assertThat(row.keySet(), hasItem("ï»¿code_insee"));
    }

    @Test
    void trailingEmptyHeaderColumnIsDropped() throws Exception {
        String csvBody = "code_insee;nom_commune;\n1001;L'Abergement-Clémenciat;\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/trailingEmptyHeader.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("trailingEmptyHeaderColumnIsDropped")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), not(hasItem("")));
        assertThat(row.keySet(), containsInAnyOrder("code_insee", "nom_commune"));
    }

    @Test
    void emptyHeaderNameInTheMiddleIsPreservedNotDropped() throws Exception {
        String csvBody = "code_insee;;nom_commune\n1001;X;L'Abergement-Clémenciat\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/middleEmptyHeader.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("emptyHeaderNameInTheMiddleIsPreservedNotDropped")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), containsInAnyOrder("code_insee", "", "nom_commune"));
        assertThat(row.get(""), is("X"));
    }

    @Test
    void multipleTrailingEmptyHeaderColumnsAreDropped() throws Exception {
        String csvBody = "code_insee;nom_commune;;\n1001;L'Abergement-Clémenciat;;\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/multiTrailingEmptyHeader.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("multipleTrailingEmptyHeaderColumnsAreDropped")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), not(hasItem("")));
        assertThat(row.keySet(), containsInAnyOrder("code_insee", "nom_commune"));
    }

    @Test
    void trailingUnnamedColumnsWithDataAreDroppedByDefault() throws Exception {
        String csvBody = "id;region;;\n1;north;100;200\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/trailingUnnamedWithData.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("trailingUnnamedColumnsWithDataAreDroppedByDefault")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), containsInAnyOrder("id", "region"));
        assertThat(row.get("id"), is("1"));
    }

    @Test
    void trailingUnnamedColumnsWithDataAreKeptWhenRename() throws Exception {
        String csvBody = "id;region;;\n1;north;100;200\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/trailingUnnamedRename.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("trailingUnnamedColumnsWithDataAreKeptWhenRename")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .onEmptyHeader(Property.ofValue(OnEmptyHeader.RENAME))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), containsInAnyOrder("id", "region", "col_2", "col_3"));
        assertThat(row.get("col_2"), is("100"));
        assertThat(row.get("col_3"), is("200"));
    }

    @Test
    void renameDisambiguatesAgainstRealColumnNamedLikeGenerated() throws Exception {
        String csvBody = "a;;col_1;d\n1;2;3;4\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/renameCollision.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("renameDisambiguatesAgainstRealColumnNamedLikeGenerated")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .onEmptyHeader(Property.ofValue(OnEmptyHeader.RENAME))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), hasSize(4));
        assertThat(row.keySet(), hasItems("a", "col_1", "d"));
        assertThat(row.get("col_1"), is("3"));
        assertThat(row.get("col_1_2"), is("2"));
    }

    @Test
    void renameNamesMiddleAndAllEmptyHeaders() throws Exception {
        String csvBody = ";b;\n1;2;3\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/renameMiddleAndEdges.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("renameNamesMiddleAndAllEmptyHeaders")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .onEmptyHeader(Property.ofValue(OnEmptyHeader.RENAME))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), containsInAnyOrder("col_0", "b", "col_2"));
        assertThat(row.get("col_0"), is("1"));
        assertThat(row.get("b"), is("2"));
        assertThat(row.get("col_2"), is("3"));
    }

    @Test
    void utf8BomAndTrailingEmptyHeaderTogether() throws Exception {
        String csvBody = "code_insee;nom_commune;\n1001;L'Abergement-Clémenciat;\n";
        byte[] utf8Bom = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

        ByteArrayOutputStream withBom = new ByteArrayOutputStream();
        withBom.write(utf8Bom);
        withBom.write(csvBody.getBytes(StandardCharsets.UTF_8));

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/bomAndTrailing.csv"),
            new ByteArrayInputStream(withBom.toByteArray())
        );

        CsvToIon reader = CsvToIon.builder()
            .id("utf8BomAndTrailingEmptyHeaderTogether")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), containsInAnyOrder("code_insee", "nom_commune"));
        assertThat(row.get("code_insee"), is("1001"));
    }

    @Test
    void allEmptyHeaderIsNotTrimmedToZeroColumns() throws Exception {
        String csvBody = ";;;\n1;2;3;4\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/allEmptyHeader.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("allEmptyHeaderIsNotTrimmedToZeroColumns")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .build();

        CsvToIon.Output out = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Object> rows;
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, out.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }

        assertThat(rows, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertThat(row.keySet(), contains(""));
        assertThat(row.get(""), is("4"));
    }

    @Test
    void rowMissingHeadersTrailingSeparatorIsFlaggedAsBadLine() throws Exception {
        String csvBody = "code_insee;nom_commune;\n1001;L'Abergement-Clémenciat\n";

        URI src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/rowMissingTrailingSeparator.csv"),
            new ByteArrayInputStream(csvBody.getBytes(StandardCharsets.UTF_8))
        );

        CsvToIon reader = CsvToIon.builder()
            .id("rowMissingHeadersTrailingSeparatorIsFlaggedAsBadLine")
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .fieldSeparator(Property.ofValue(';'))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.ERROR))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());

        Throwable thrown = assertThrows(RuntimeException.class, () -> reader.run(runContext));
        assertThat(thrown.getMessage(), containsString("Bad line encountered (field count mismatch): Expected 3, got 2 fields."));
    }

    @Test
    void badLinesErrorThrows() throws Exception {
        String csv = "header1,header2\nvalue1,value2\nvalue3,value4,value5\nvalue6,value7";
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

        assertThat(recordsWarn.getValue(), is(3D)); // value1, value6 and value11 remain

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

        assertThat(recordsSkip.getValue(), is(3D)); // value1, value6 and value11 remain
    }

    @Test
    void testCsvWithBadRows() throws Exception {
        String csv = "name,age,city\n" +
            "Alice,New York\n" + // less column → bad row
            "Bob,25,London,extra\n" + // extra column → bad row
            "Charlie,35,Paris"; // correct row

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

    @Test
    @Timeout(30)
    void badQuotesPreservesSubsequentRows() throws Exception {
        var csv = "k,v\n10,alpha\n20,\"be\"ta\n30,gamma\n40,delta\n50,eps\n";
        var src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        // Test with onBadLines: SKIP
        var readerSkip = CsvToIon.builder()
            .id(IdUtils.create())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        var runContextSkip = TestsUtils.mockRunContext(runContextFactory, readerSkip, ImmutableMap.of());
        var outputSkip = readerSkip.run(runContextSkip);

        var recordsSkip = (Counter) runContextSkip.metrics()
            .stream()
            .filter(metric -> metric.getName().equals("records"))
            .findFirst()
            .orElseThrow();

        assertThat(recordsSkip.getValue(), is(4D));
        assertThat(outputSkip.getSize(), is(greaterThan(0L)));

        List<Object> rowsSkip;
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, outputSkip.getUri())) {
            rowsSkip = FileSerde.readAll(in).collectList().block();
        }
        assertThat(rowsSkip, hasSize(4));
        assertThat(rowsSkip.getFirst(), is((Object) Map.of("k", "10", "v", "alpha")));
        assertThat(rowsSkip.get(1), is((Object) Map.of("k", "30", "v", "gamma")));
        assertThat(rowsSkip.get(2), is((Object) Map.of("k", "40", "v", "delta")));
        assertThat(rowsSkip.get(3), is((Object) Map.of("k", "50", "v", "eps")));

        // Test with onBadLines: WARN
        var readerWarn = CsvToIon.builder()
            .id(IdUtils.create())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        var runContextWarn = TestsUtils.mockRunContext(runContextFactory, readerWarn, ImmutableMap.of());
        var warnings = attachLogCapture(runContextWarn);
        var outputWarn = readerWarn.run(runContextWarn);

        assertThat(
            warnings.list.stream()
                .filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("Bad line encountered"))
                .count(),
            is(1L)
        );

        var recordsWarn = (Counter) runContextWarn.metrics()
            .stream()
            .filter(metric -> metric.getName().equals("records"))
            .findFirst()
            .orElseThrow();

        assertThat(recordsWarn.getValue(), is(4D));
        assertThat(outputWarn.getSize(), is(greaterThan(0L)));

        List<Object> rowsWarn;
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, outputWarn.getUri())) {
            rowsWarn = FileSerde.readAll(in).collectList().block();
        }
        assertThat(rowsWarn, hasSize(4));
        assertThat(rowsWarn.getFirst(), is((Object) Map.of("k", "10", "v", "alpha")));
        assertThat(rowsWarn.get(1), is((Object) Map.of("k", "30", "v", "gamma")));
        assertThat(rowsWarn.get(2), is((Object) Map.of("k", "40", "v", "delta")));
        assertThat(rowsWarn.get(3), is((Object) Map.of("k", "50", "v", "eps")));
    }

    @Test
    @Timeout(30)
    void multiLineQuotedRecordsWithBadLine() throws Exception {
        var csv = "k,v\n10,\"hello\nworld\"\n20,\"be\"ta\n30,\"multi\nline\nrecord\"\n";
        var src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        var reader = CsvToIon.builder()
            .id(IdUtils.create())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(true))
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        var output = reader.run(runContext);

        var records = (Counter) runContext.metrics()
            .stream()
            .filter(metric -> metric.getName().equals("records"))
            .findFirst()
            .orElseThrow();

        assertThat(records.getValue(), is(2D));

        List<Object> rows;
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
            rows = FileSerde.readAll(in).collectList().block();
        }
        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "10", "v", "hello\nworld")));
        assertThat(rows.get(1), is((Object) Map.of("k", "30", "v", "multi\nline\nrecord")));
    }

    private List<Object> readSkipping(String csv, boolean header, OnBadLines onBadLines, Integer maxRecordSize) throws Exception {
        var src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        var builder = CsvToIon.builder()
            .id(IdUtils.create())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(header))
            .onBadLines(Property.ofValue(onBadLines));
        if (maxRecordSize != null) {
            builder.maxFieldSize(Property.ofValue(maxRecordSize)).maxRecordSize(Property.ofValue(maxRecordSize));
        }
        var reader = builder.build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        var output = reader.run(runContext);
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
            return FileSerde.readAll(in).collectList().block();
        }
    }

    @Test
    @Timeout(30)
    void multiLineRecordOfThreeLinesWithoutHeaderIsKept() throws Exception {
        var rows = readSkipping("1,\"multi\nline\nrecord\"\n2,ok\n", false, OnBadLines.SKIP, null);

        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) List.of("1", "multi\nline\nrecord")));
        assertThat(rows.get(1), is((Object) List.of("2", "ok")));
    }

    @Test
    @Timeout(30)
    void multiLineRecordMiddleLineWithHeaderFieldCountIsKept() throws Exception {
        var rows = readSkipping("k,v\n1,\"123 Main St\nApt 4, Floor 2\nCity\"\n2,\"be\"ta\n3,z\n", true, OnBadLines.WARN, null);

        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "1", "v", "123 Main St\nApt 4, Floor 2\nCity")));
        assertThat(rows.get(1), is((Object) Map.of("k", "3", "v", "z")));
    }

    @Test
    @Timeout(30)
    void singleColumnMultiLineRecordIsKept() throws Exception {
        var rows = readSkipping("v\n\"a\nb\nc\"\nd\n", true, OnBadLines.SKIP, null);

        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) Map.of("v", "a\nb\nc")));
        assertThat(rows.get(1), is((Object) Map.of("v", "d")));
    }

    @Test
    @Timeout(30)
    void headerEndingWithSeparatorKeepsRowsAfterUnclosedQuote() throws Exception {
        var rows = readSkipping("a,b,\n1,x,\n2,\"bad,\n3,y,\n4,z,\n", true, OnBadLines.SKIP, null);

        assertThat(rows, hasSize(3));
        assertThat(rows.getFirst(), is((Object) Map.of("a", "1", "b", "x")));
        assertThat(rows.get(1), is((Object) Map.of("a", "3", "b", "y")));
        assertThat(rows.get(2), is((Object) Map.of("a", "4", "b", "z")));
    }

    @Test
    @Timeout(30)
    void overlongLineIsDroppedAsBadLine() throws Exception {
        var rows = readSkipping("k,v\n1,a\n2," + "x".repeat(5000) + "\n3,c\n", true, OnBadLines.SKIP, 1000);

        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "1", "v", "a")));
        assertThat(rows.get(1), is((Object) Map.of("k", "3", "v", "c")));
    }

    @Test
    @Timeout(30)
    void longUnclosedQuoteIsLinear() throws Exception {
        var csv = new StringBuilder("k,v\n0,\"open\n");
        for (int i = 1; i <= 20000; i++) {
            csv.append(i).append(",row\n");
        }
        var rows = readSkipping(csv.toString(), true, OnBadLines.SKIP, null);

        assertThat(rows, hasSize(20000));
    }

    private List<Object> readRows(String csv, boolean header, OnBadLines onBadLines, int skipRows, Integer maxFieldSize) throws Exception {
        var src = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".csv"),
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))
        );

        var builder = CsvToIon.builder()
            .id(IdUtils.create())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(src.toString()))
            .header(Property.ofValue(header))
            .skipEmptyRows(Property.ofValue(false))
            .skipRows(Property.ofValue(skipRows))
            .onBadLines(Property.ofValue(onBadLines));
        if (maxFieldSize != null) {
            builder.maxFieldSize(Property.ofValue(maxFieldSize)).maxRecordSize(Property.ofValue(maxFieldSize * 4));
        }
        var reader = builder.build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        var output = reader.run(runContext);
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
            return FileSerde.readAll(in).collectList().block();
        }
    }

    @Test
    @Timeout(30)
    void errorModeKeepsUnclosedQuoteAtEndOfInput() throws Exception {
        var rows = readRows("k,v\n1,a\n2,\"open", true, OnBadLines.ERROR, 0, null);

        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "1", "v", "a")));
        assertThat(rows.getLast(), is((Object) Map.of("k", "2", "v", "open")));
    }

    @Test
    @Timeout(30)
    void crlfInsideQuotedFieldIsPreserved() throws Exception {
        var csv = "k,v\r\n1,\"a\r\nb\"\r\n2,\"be\"ta\r\n3,c\r\n";
        var expected = Map.of("k", "1", "v", "a\r\nb");

        assertThat(readRows(csv.replace("2,\"be\"ta\r\n", ""), true, OnBadLines.ERROR, 0, null).getFirst(), is((Object) expected));

        var skip = readRows(csv, true, OnBadLines.SKIP, 0, null);
        assertThat(skip, hasSize(2));
        assertThat(skip.getFirst(), is((Object) expected));
        assertThat(skip.getLast(), is((Object) Map.of("k", "3", "v", "c")));
    }

    @Test
    @Timeout(30)
    void blankLineWithoutHeaderYieldsEmptyRowLikeErrorMode() throws Exception {
        var csv = "1,a\n\n2,b\n";
        var error = readRows(csv, false, OnBadLines.ERROR, 0, null);

        assertThat(error, hasSize(3));
        assertThat(readRows(csv, false, OnBadLines.SKIP, 0, null), is(error));
        assertThat(readRows(csv, false, OnBadLines.WARN, 0, null), is(error));
    }

    @Test
    @Timeout(30)
    void badPreambleLineCountsTowardSkipRows() throws Exception {
        var rows = readRows("\"junk\"x\nk,v\n1,a\n", true, OnBadLines.SKIP, 1, null);

        assertThat(rows, hasSize(1));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "1", "v", "a")));
    }

    @Test
    @Timeout(30)
    void goodPreambleLineCountsTowardSkipRows() throws Exception {
        var rows = readRows("preamble\nk,v\n1,a\n", true, OnBadLines.SKIP, 1, null);

        assertThat(rows, hasSize(1));
        assertThat(rows.getFirst(), is((Object) Map.of("k", "1", "v", "a")));
    }

    @Test
    @Timeout(30)
    void openQuoteIsBoundedByMaxFieldSize() throws Exception {
        var csv = "1,a\n2,\"open\n" + "x\n".repeat(2000) + "3,c\n";
        var rows = readRows(csv, false, OnBadLines.SKIP, 0, 100);

        assertThat(rows, hasSize(2002));
        assertThat(rows.getFirst(), is((Object) List.of("1", "a")));
        assertThat(rows.getLast(), is((Object) List.of("3", "c")));
    }

    // runContext.logger() returns a Logback logger from an isolated LoggerContext; attach directly to capture logs.
    private static final class CapturingAppender extends AppenderBase<ILoggingEvent> {
        final List<ILoggingEvent> list = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            list.add(event);
        }
    }

    private static CapturingAppender attachLogCapture(RunContext runContext) {
        var contextLogger = (Logger) runContext.logger();
        contextLogger.setLevel(Level.DEBUG);
        var appender = new CapturingAppender();
        appender.setContext(contextLogger.getLoggerContext());
        appender.start();
        contextLogger.addAppender(appender);
        return appender;
    }
}
