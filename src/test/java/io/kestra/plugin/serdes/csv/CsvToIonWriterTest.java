package io.kestra.plugin.serdes.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

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
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.OnBadLines;
import io.kestra.plugin.serdes.OnEmptyHeader;
import io.kestra.plugin.serdes.SerdesUtils;

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
    void utf8BomInHeaderIsStripped() throws Exception {
        // GitHub issue #17646: a UTF-8 BOM at the start of the file was leaking into the first
        // header's name, causing it to be quoted differently from every other field in the Ion output.
        String csvBody = "code_insee;nom_commune\n1001;L'Abergement-Clémenciat\n";
        byte[] utf8Bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

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
    void trailingEmptyHeaderColumnIsDropped() throws Exception {
        // GitHub issue #17646: a trailing field separator on the header line produced a
        // nameless field ('':"") on every row in the Ion output.
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
        // Only *trailing* empty header names are treated as a trailing-separator artifact. An empty
        // name in the middle of the header carries real data and is a structural property of the
        // source file, not an artifact, so it must be preserved rather than silently dropped.
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
        // Two trailing separators in a row should drop both empty-named trailing columns, not just one.
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
        // Default onEmptyHeader=DROP: trailing columns with an empty header name are dropped even
        // when the rows carry values there. Documents that data in those columns is not emitted.
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
        // onEmptyHeader=RENAME: keep every column and give unnamed ones generated names (col_2,
        // col_3, ...) so no data is lost and downstream conversions get valid column names.
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
        // RENAME must not overwrite a real column that happens to be named like a generated one.
        // Here index 1 is empty (would generate "col_1") and index 2 is literally named "col_1";
        // the generated name is disambiguated so all four columns and their values survive.
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
        // 4 distinct keys: the real "col_1" is preserved, the generated one gets "col_1_2".
        assertThat(row.keySet(), hasSize(4));
        assertThat(row.keySet(), hasItems("a", "col_1", "d"));
        assertThat(row.get("col_1"), is("3"));
        assertThat(row.get("col_1_2"), is("2"));
    }

    @Test
    void renameNamesMiddleAndAllEmptyHeaders() throws Exception {
        // RENAME renames every empty header name, including one in the middle and the all-empty case,
        // so no column collapses to an unusable "" name and no data is lost.
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
        // GitHub issue #17646 as reported: the source file had both a UTF-8 BOM and a trailing
        // separator. This is the combined regression guard for the exact reported shape.
        String csvBody = "code_insee;nom_commune;\n1001;L'Abergement-Clémenciat;\n";
        byte[] utf8Bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

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
        // Edge case: if the whole header is empty (e.g. ";;;"), trimming every column would leave
        // zero columns and every row would collapse to an empty record. There's no "real" header
        // left to anchor the trim against, so nothing is dropped in this case.
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
        // Every column is named "" so they collide into a single map key; the point of this test
        // is that the row is NOT an empty {} record with the data silently discarded.
        assertThat(row.keySet(), contains(""));
        assertThat(row.get(""), is("4"));
    }

    @Test
    void rowMissingHeadersTrailingSeparatorIsFlaggedAsBadLine() throws Exception {
        // Intended behavior: the field-count check validates against the RAW header field count
        // (including the trailing unnamed column that gets dropped from the output), so a data row
        // that leaves off the header's trailing separator is correctly flagged as a bad line rather
        // than silently accepted.
        String csvBody = "code_insee;nom_commune;\n1001;L'Abergement-Clémenciat\n"; // data row omits the trailing ';'

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
}
