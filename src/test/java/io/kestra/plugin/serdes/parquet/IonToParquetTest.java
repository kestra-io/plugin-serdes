package io.kestra.plugin.serdes.parquet;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.OnBadLines;

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class IonToParquetTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void convertWithExplicitSchema() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("String", "string")
                    .put("Int", 2)
                    .put("Float", 3.2F)
                    .put("Double", 3.2D)
                    .put("Instant", Instant.now())
                    .put("ZonedDateTime", ZonedDateTime.parse("2021-08-02T12:00:00+02:00"))
                    .put("LocalDateTime", LocalDateTime.now())
                    .put("OffsetDateTime", OffsetDateTime.now())
                    .put("LocalDate", LocalDate.now())
                    .put("LocalTime", LocalTime.now())
                    .put("OffsetTime", OffsetTime.now())
                    .put("Date", new Date())
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToParquet writer = IonToParquet.builder()
                .id(IonToParquet.class.getSimpleName())
                .type(IonToParquet.class.getName())
                .from(Property.ofValue(uri.toString()))
                .schema(
                    IOUtils.toString(
                        Objects.requireNonNull(IonToParquetTest.class.getClassLoader().getResource("avro/all.avsc")),
                        StandardCharsets.UTF_8
                    )
                )
                .timeZoneId(Property.ofValue("UTC"))
                .build();

            IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            // Verify output by reading back
            ParquetToIon reader = ParquetToIon.builder()
                .id(ParquetToIon.class.getSimpleName())
                .type(ParquetToIon.class.getName())
                .from(Property.ofValue(writerOutput.getUri().toString()))
                .build();

            ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            List<Map<String, Object>> result = new ArrayList<>();
            FileSerde.read(storageInterface.get(TenantService.MAIN_TENANT, null, readerOutput.getUri()), r -> result.add((Map<String, Object>) r));

            assertThat(result.size(), is(1));
            assertThat(result.get(0).get("String"), is("string"));
            assertThat(writerOutput.getSize(), is(1L));
            assertThat(readerOutput.getSize(), is(1L));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void convertWithInferredSchema() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("String", "string")
                    .put("Int", 2)
                    .put("Float", 3.2F)
                    .put("Double", 3.2D)
                    .put("Instant", Instant.now())
                    .put("ZonedDateTime", ZonedDateTime.parse("2021-08-02T12:00:00+02:00"))
                    .put("LocalDateTime", LocalDateTime.now())
                    .put("OffsetDateTime", OffsetDateTime.now())
                    .put("LocalDate", LocalDate.now())
                    .put("LocalTime", LocalTime.now())
                    .put("OffsetTime", OffsetTime.now())
                    .put("Date", new Date())
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            // No schema provided - should infer
            IonToParquet writer = IonToParquet.builder()
                .id(IonToParquet.class.getSimpleName())
                .type(IonToParquet.class.getName())
                .from(Property.ofValue(uri.toString()))
                .schema(null)
                .timeZoneId(Property.ofValue("UTC"))
                .build();

            IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            // Verify output by reading back
            ParquetToIon reader = ParquetToIon.builder()
                .id(ParquetToIon.class.getSimpleName())
                .type(ParquetToIon.class.getName())
                .from(Property.ofValue(writerOutput.getUri().toString()))
                .build();

            ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            List<Map<String, Object>> result = new ArrayList<>();
            FileSerde.read(storageInterface.get(TenantService.MAIN_TENANT, null, readerOutput.getUri()), r -> result.add((Map<String, Object>) r));

            assertThat(result.size(), is(1));
            assertThat(result.get(0).get("String"), is("string"));
        }
    }

    @Test
    void inferenceFailsOnEmptyFile() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_empty_", ".ion");
        // Write nothing to the file - it's empty

        URI uri;
        try (InputStream inputStream = new FileInputStream(tempFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), inputStream);
        }

        IonToParquet writer = IonToParquet.builder()
            .id(IonToParquet.class.getSimpleName())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(null) // No schema - inference required
            .build();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );

        assertThat(exception.getMessage(), is("Cannot infer Avro schema from ION input: the file appears to be empty or contains no valid records."));
    }

    @SuppressWarnings("unchecked")
    @Test
    void convertMultipleRowsWithInferredSchema() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_multi_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.of("id", 1, "name", "Alice", "active", true),
                ImmutableMap.of("id", 2, "name", "Bob", "active", false),
                ImmutableMap.of("id", 3, "name", "Charlie", "active", true)
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToParquet writer = IonToParquet.builder()
                .id(IonToParquet.class.getSimpleName())
                .type(IonToParquet.class.getName())
                .from(Property.ofValue(uri.toString()))
                .schema(null) // Infer schema
                .build();

            IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            // Verify by reading back
            ParquetToIon reader = ParquetToIon.builder()
                .id(ParquetToIon.class.getSimpleName())
                .type(ParquetToIon.class.getName())
                .from(Property.ofValue(writerOutput.getUri().toString()))
                .build();

            ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            List<Map<String, Object>> result = new ArrayList<>();
            FileSerde.read(storageInterface.get(TenantService.MAIN_TENANT, null, readerOutput.getUri()), r -> result.add((Map<String, Object>) r));

            assertThat(result.size(), is(3));
            assertThat(result.get(0).get("name"), is("Alice"));
            assertThat(result.get(1).get("name"), is("Bob"));
            assertThat(result.get(2).get("name"), is("Charlie"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void inferAllFieldsTrueScansAllRowsForDateField() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_infer_all_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            // Rows 1–100: date field is null
            IntStream.rangeClosed(1, 100).boxed()
                .forEach(throwConsumer(i ->
                {
                    var row = new HashMap<String, Object>();
                    row.put("id", i);
                    row.put("event_date", null);
                    FileSerde.write(output, row);
                }));
            // Row 101+: date field is non-null
            IntStream.rangeClosed(101, 110).boxed()
                .forEach(
                    throwConsumer(
                        i -> FileSerde.write(
                            output,
                            Map.of("id", i, "event_date", LocalDate.of(2024, 1, i - 100))
                        )
                    )
                );
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(null)
            .inferAllFields(Property.ofValue(true))
            .build();

        // Must succeed: all rows are scanned so event_date is typed correctly (not NULL)
        IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        ParquetToIon reader = ParquetToIon.builder()
            .id(IdUtils.create())
            .type(ParquetToIon.class.getName())
            .from(Property.ofValue(writerOutput.getUri().toString()))
            .build();

        ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.read(storageInterface.get(TenantService.MAIN_TENANT, null, readerOutput.getUri()), r -> result.add((Map<String, Object>) r));

        assertThat(result.size(), is(110));
        // Rows 101–110 must have a non-null date
        assertThat(result.stream().filter(r -> r.get("event_date") != null).count(), greaterThan(0L));
    }

    @Test
    void inferAllFieldsFalseFailsWhenDateFieldNullInFirstRows() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_infer_limited_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            // Rows 1–100: date field is null → inferred as NULL type
            IntStream.rangeClosed(1, 100).boxed()
                .forEach(throwConsumer(i ->
                {
                    var row = new HashMap<String, Object>();
                    row.put("id", i);
                    row.put("event_date", null);
                    FileSerde.write(output, row);
                }));
            // Row 101+: date field is non-null → conflicts with NULL schema
            IntStream.rangeClosed(101, 110).boxed()
                .forEach(
                    throwConsumer(
                        i -> FileSerde.write(
                            output,
                            Map.of("id", i, "event_date", LocalDate.of(2024, 1, i - 100))
                        )
                    )
                );
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(null)
            .inferAllFields(Property.ofValue(false))
            .numberOfRowsToScan(Property.ofValue(100))
            .build();

        // Must throw: event_date was inferred as NULL but row 101 has a real date value
        RuntimeException ex = assertThrows(
            RuntimeException.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );
        Throwable root = ex;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertThat(root.getMessage(), containsString("Unknown type for null values"));
    }

    private static final String NON_NULLABLE_INT_SCHEMA = """
        {
          "type": "record",
          "name": "BadLine",
          "namespace": "com.example.badline",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "s", "type": "string"}
          ]
        }""";

    private static final String LOGICAL_TYPES_SCHEMA = """
        {
          "type": "record",
          "name": "LogicalTypes",
          "namespace": "com.example.logical",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
            {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "eventDate", "type": {"type": "int", "logicalType": "date"}},
            {"name": "externalId", "type": {"type": "string", "logicalType": "uuid"}}
          ]
        }""";

    private static Map<String, Object> logicalRow(int id) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("amount", new BigDecimal("12.34"));
        row.put("createdAt", Instant.parse("2024-01-01T12:34:56Z"));
        row.put("eventDate", LocalDate.of(2024, 1, 1));
        row.put("externalId", UUID.randomUUID().toString());
        return row;
    }

    private URI uploadIonRows(List<Map<String, Object>> rows) throws Exception {
        File tempFile = File.createTempFile("iontoparquet_onbadlines_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            rows.forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    private static List<Map<String, Object>> rowsWithOneBadId() {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(ImmutableMap.of("id", 1, "s", "a"));
        Map<String, Object> bad = new HashMap<>();
        bad.put("id", null); // null into a non-nullable "int" field
        bad.put("s", "b");
        rows.add(bad);
        rows.add(ImmutableMap.of("id", 3, "s", "c"));
        return rows;
    }

    // runContext.logger() returns a Logback logger from an isolated LoggerContext; attach directly to capture logs.
    private static ListAppender<ILoggingEvent> attachLogCapture(RunContext runContext) {
        Logger contextLogger = (Logger) runContext.logger();
        contextLogger.setLevel(Level.DEBUG);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.setContext(contextLogger.getLoggerContext());
        listAppender.start();
        contextLogger.addAppender(listAppender);
        return listAppender;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readBackAsRows(URI parquetUri) throws Exception {
        ParquetToIon reader = ParquetToIon.builder()
            .id(IdUtils.create())
            .type(ParquetToIon.class.getName())
            .from(Property.ofValue(parquetUri.toString()))
            .build();

        ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.read(storageInterface.get(TenantService.MAIN_TENANT, null, readerOutput.getUri()), r -> result.add((Map<String, Object>) r));
        return result;
    }

    @Test
    void warnSkipsBadRowLogsWarningAndSucceeds() throws Exception {
        URI uri = uploadIonRows(rowsWithOneBadId());

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is(2L));
        assertThat(
            listAppender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("onBadLines=WARN")),
            is(true)
        );

        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
        assertThat(result.stream().map(r -> r.get("id")).toList(), is(List.of(1, 3)));
    }

    @Test
    void warnLogsTruncateOversizedRecordButKeepFieldAndSchemaNameIntact() throws Exception {
        String hugeValue = "s".repeat(5000);
        Map<String, Object> bad = new HashMap<>();
        bad.put("id", null); // null into a non-nullable "int" field
        bad.put("s", hugeValue);
        URI uri = uploadIonRows(List.of(bad));

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        writer.run(runContext);

        String message = listAppender.list.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains("onBadLines=WARN"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected a WARN log for the bad record"));

        assertThat(message, containsString("field 'id'"));
        assertThat(message, containsString("schema 'BadLine'"));
        assertThat(message, containsString("(truncated)"));
        assertThat(message.length(), lessThan(hugeValue.length()));
    }

    @Test
    void skipDropsBadRowSilentlyAndProducesReadableParquet() throws Exception {
        URI uri = uploadIonRows(rowsWithOneBadId());

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is(2L));
        assertThat(listAppender.list.isEmpty(), is(true));

        // round-trip through ParquetToIon: this is what actually proves the row group was not left corrupted
        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
        assertThat(result.stream().map(r -> r.get("id")).toList(), is(List.of(1, 3)));
    }

    @Test
    void allRowsBadUnderSkipProducesEmptyButValidParquet() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> bad = new HashMap<>();
            bad.put("id", null);
            bad.put("s", "x" + i);
            rows.add(bad);
        }
        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(writerOutput.getSize(), is(0L));
        assertThat(readBackAsRows(writerOutput.getUri()), is(List.of()));
    }

    @Test
    void warnSkipsBadRowInNestedRecordField() throws Exception {
        String nestedSchema = """
            {
              "type": "record",
              "name": "WithAddress",
              "namespace": "com.example.badline",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "address", "type": {
                  "type": "record",
                  "name": "Address",
                  "fields": [
                    {"name": "zip", "type": "int"}
                  ]
                }}
              ]
            }""";

        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "address", Map.of("zip", 75001)));
        Map<String, Object> badZip = new HashMap<>();
        badZip.put("zip", null); // null into a non-nullable nested "int" field
        Map<String, Object> badRow = new HashMap<>();
        badRow.put("id", 2);
        badRow.put("address", badZip);
        rows.add(badRow);
        rows.add(Map.of("id", 3, "address", Map.of("zip", 75002)));

        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(nestedSchema)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(writerOutput.getSize(), is(2L));
        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
    }

    private static final String ARRAY_OF_RECORDS_SCHEMA = """
        {
          "type": "record",
          "name": "WithItems",
          "namespace": "com.example.badline",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "items", "type": {"type": "array", "items": {
              "type": "record",
              "name": "Item",
              "fields": [
                {"name": "score", "type": "int"}
              ]
            }}}
          ]
        }""";

    // Regression for the gate missing records nested inside an ARRAY field: AvroConverter.complexArray resolves
    // a bad element the same way fromMap does (null the field, keep the record), so this row must still fail the
    // pre-write gate instead of reaching the writer. Must fail against the pre-fix gate, which only checked
    // `instanceof GenericData.Record` on direct field values, never descending into List/Map contents.
    @Test
    void warnSkipsBadRowInArrayOfRecordsField() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "items", List.of(Map.of("score", 10), Map.of("score", 20))));
        Map<String, Object> badItem = new HashMap<>();
        badItem.put("score", null); // null into the non-nullable "score" field of an array element
        rows.add(Map.of("id", 2, "items", List.of(Map.of("score", 30), badItem)));
        rows.add(Map.of("id", 3, "items", List.of(Map.of("score", 40))));

        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(ARRAY_OF_RECORDS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is(2L));
        assertThat(
            listAppender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("items[1].score")),
            is(true)
        );
        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
        assertThat(result.stream().map(r -> r.get("id")).toList(), is(List.of(1, 3)));
    }

    private static final String MAP_OF_RECORDS_SCHEMA = """
        {
          "type": "record",
          "name": "WithByKey",
          "namespace": "com.example.badline",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "byKey", "type": {"type": "map", "values": {
              "type": "record",
              "name": "Entry",
              "fields": [
                {"name": "score", "type": "int"}
              ]
            }}}
          ]
        }""";

    // Same gap as above but for a MAP field: AvroConverter.complexMap routes each value through the same
    // fromMap(...) call, so a null in a non-nullable sub-field of a map value is just as invisible to a gate
    // that only recurses into direct field values.
    @Test
    void warnSkipsBadRowInMapOfRecordsField() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "byKey", Map.of("a", Map.of("score", 100))));
        Map<String, Object> badEntry = new HashMap<>();
        badEntry.put("score", null); // null into the non-nullable "score" field of a map value
        rows.add(Map.of("id", 2, "byKey", Map.of("a", badEntry)));
        rows.add(Map.of("id", 3, "byKey", Map.of("a", Map.of("score", 200))));

        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(MAP_OF_RECORDS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is(2L));
        assertThat(
            listAppender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("byKey{'a'}.score")),
            is(true)
        );
        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
        assertThat(result.stream().map(r -> r.get("id")).toList(), is(List.of(1, 3)));
    }

    private static final String NESTED_LOGICAL_CONTAINERS_SCHEMA = """
        {
          "type": "record",
          "name": "NestedLogical",
          "namespace": "com.example.logical",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "items", "type": {"type": "array", "items": {
              "type": "record",
              "name": "Item",
              "fields": [
                {"name": "externalId", "type": {"type": "string", "logicalType": "uuid"}}
              ]
            }}},
            {"name": "byKey", "type": {"type": "map", "values": {
              "type": "record",
              "name": "Entry",
              "fields": [
                {"name": "eventDate", "type": {"type": "int", "logicalType": "date"}}
              ]
            }}}
          ]
        }""";

    private static Map<String, Object> validNestedLogicalRow(int id) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("items", List.of(
            Map.of("externalId", UUID.randomUUID().toString()),
            Map.of("externalId", UUID.randomUUID().toString())
        ));
        row.put("byKey", Map.of("a", Map.of("eventDate", LocalDate.of(2024, 1, 1))));
        return row;
    }

    // Counterpart to the logical-type gate regression, for container fields: an array-of-records and a
    // map-of-records whose sub-fields legitimately hold non-null logical-typed values (uuid, date) must not
    // be mistaken for the "null in a non-nullable field" bad-record shape, so every row must survive with zero
    // warnings. Stops a future over-broad container gate from silently dropping valid nested data.
    @Test
    void warnKeepsAllRowsWhenArrayAndMapOfRecordsAreValidWithLogicalSubfields() throws Exception {
        List<Map<String, Object>> rows = IntStream.rangeClosed(1, 5).mapToObj(IonToParquetTest::validNestedLogicalRow).toList();
        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NESTED_LOGICAL_CONTAINERS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .timeZoneId(Property.ofValue("UTC"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is((long) rows.size()));
        assertThat(listAppender.list.isEmpty(), is(true));
        assertThat(readBackAsRows(writerOutput.getUri()).size(), is(rows.size()));
    }

    @Test
    void errorOnBadRowFailsUnchanged() throws Exception {
        URI uri = uploadIonRows(rowsWithOneBadId());

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.ERROR))
            .build();

        RuntimeException ex = assertThrows(
            RuntimeException.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );

        // unchanged: the exact same wrapping chain and root cause as before this fix, since validation is
        // skipped entirely under ERROR and the failure still originates from field conversion, not from writer.write()
        Throwable current = ex;
        boolean sawIllegalRowConvertion = false;
        while (current.getCause() != null) {
            current = current.getCause();
            sawIllegalRowConvertion |= current instanceof io.kestra.plugin.serdes.avro.AvroConverter.IllegalRowConvertion;
        }
        assertThat(sawIllegalRowConvertion, is(true));
        assertThat(current, instanceOf(NullPointerException.class));
    }

    // Regression for the GenericData.validate() gate: it switches on each field's physical Avro type and ignores
    // registered logical-type conversions, so it used to reject the BigDecimal/Instant/LocalDate/UUID values
    // AvroConverter legitimately produces for decimal/timestamp/date/uuid fields, dropping every row of any
    // schema using a logical type -- even fully valid data. This test must fail against that gate.
    @Test
    void warnKeepsAllRowsWhenSchemaHasLogicalTypesAndDataIsValid() throws Exception {
        List<Map<String, Object>> rows = IntStream.rangeClosed(1, 5).mapToObj(IonToParquetTest::logicalRow).toList();
        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(LOGICAL_TYPES_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .timeZoneId(Property.ofValue("UTC"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToParquet.Output writerOutput = writer.run(runContext);

        assertThat(writerOutput.getSize(), is((long) rows.size()));
        assertThat(listAppender.list.isEmpty(), is(true));
        assertThat(readBackAsRows(writerOutput.getUri()).size(), is(rows.size()));
    }

    @Test
    void warnSkipsGenuinelyBadRowButKeepsValidLogicalTypedRows() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(logicalRow(1));
        Map<String, Object> bad = logicalRow(2);
        bad.put("externalId", null); // null into a non-nullable "uuid" field
        rows.add(bad);
        rows.add(logicalRow(3));

        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(LOGICAL_TYPES_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .timeZoneId(Property.ofValue("UTC"))
            .build();

        IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(writerOutput.getSize(), is(2L));
        List<Map<String, Object>> result = readBackAsRows(writerOutput.getUri());
        assertThat(result.size(), is(2));
        assertThat(result.stream().map(r -> r.get("id")).toList(), is(List.of(1, 3)));
    }

    // A "fixed"-backed decimal whose unscaled value does not fit in the declared byte size passes conversion and
    // the null-field gate (it's a legitimate, non-null BigDecimal) but is rejected by Parquet's encoder at write
    // time, only once consumer.accept(datum) is actually called.
    private static final String FIXED_DECIMAL_SCHEMA = """
        {
          "type": "record",
          "name": "FixedDecimal",
          "namespace": "com.example.fixeddecimal",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "amount", "type": {"type": "fixed", "name": "AmountFixed", "size": 2, "logicalType": "decimal", "precision": 4, "scale": 2}}
          ]
        }""";

    // Regression coverage for the writer-abort cascade (peer review finding 2, non-blocking): the pre-write gate
    // only catches the "null into a non-nullable field" failure mode. A decimal that overflows its declared fixed
    // size is rejected by Parquet's encoder, not by the gate. InternalParquetRecordWriter marks itself aborted on
    // that failure, so the *next* row's write() throws a fresh "Writer has been aborted..." IOException -- which
    // isIOFailure() correctly treats as an infrastructure failure and escalates, hard-failing the task even under
    // WARN, but attributing the failure to the row after the one that actually caused it. This is documented,
    // accepted behaviour, not fixed here: see finding 2 on PR #394.
    @Test
    void writerAbortAfterEncodeFailureHardFailsSubsequentRowUnderWarn() throws Exception {
        List<Map<String, Object>> rows = List.of(
            ImmutableMap.of("id", 1, "amount", new BigDecimal("12.34")),
            ImmutableMap.of("id", 2, "amount", new BigDecimal("999999.99")), // unscaled value overflows the 2-byte fixed size
            ImmutableMap.of("id", 3, "amount", new BigDecimal("56.78"))
        );

        URI uri = uploadIonRows(rows);

        IonToParquet writer = IonToParquet.builder()
            .id(IdUtils.create())
            .type(IonToParquet.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(FIXED_DECIMAL_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RuntimeException ex = assertThrows(
            RuntimeException.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );

        Throwable root = ex;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertThat(root, instanceOf(IOException.class));
        assertThat(root.getMessage(), containsString("aborted"));
    }
}
