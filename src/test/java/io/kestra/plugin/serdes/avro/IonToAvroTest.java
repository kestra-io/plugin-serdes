package io.kestra.plugin.serdes.avro;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

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
import io.kestra.plugin.serdes.csv.IonToCsv;

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class IonToAvroTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void map() throws Exception {
        test("csv/insurance_sample.ion");
    }

    @Test
    void array() throws Exception {
        test("csv/insurance_sample_array.ion");
    }

    void test(String file) throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(
                new File(
                    Objects.requireNonNull(
                        IonToAvroTest.class.getClassLoader()
                            .getResource(file)
                    )
                        .toURI()
                )
            )
        );

        IonToAvro task = IonToAvro.builder()
            .id(IonToAvroTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(source.toString()))
            .inferAllFields(Property.ofValue(false))
            .schema(
                Files.asCharSource(
                    new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader().getResource("csv/insurance_sample.avsc")).toURI()),
                    Charsets.UTF_8
                ).read()
            )
            .build();

        IonToAvro.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        int recordCount = IonToAvroTest.avroSize(
            new FileInputStream(
                new File(
                    Objects.requireNonNull(
                        IonToAvroTest.class.getClassLoader()
                            .getResource("csv/insurance_sample.avro")
                    )
                        .toURI()
                )
            )
        );
        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri())),
            is(recordCount)
        );
        assertThat(run.getSize(), is((long) recordCount));
    }

    public static int avroSize(InputStream inputStream) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        AtomicInteger i = new AtomicInteger();
        dataFileReader.forEach(genericRecord -> i.getAndIncrement());

        return i.get();
    }

    @Test
    void ion() throws Exception {
        runIonToAvroTestWithSchema(
            IOUtils.toString(
                Objects.requireNonNull(IonToAvroTest.class.getClassLoader().getResource("avro/all.avsc")),
                StandardCharsets.UTF_8
            )
        );
    }

    @Test
    void ionWithoutSchema() throws Exception {
        runIonToAvroTestWithSchema(null);
    }

    void runIonToAvroTestWithSchema(String schema) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("String", "string")
                    .put("Int", 2)
                    .put("Float", 3.2F)
                    .put("Double", 3.2D)
                    .put("Instant", Instant.now())
                    .put("ZonedDateTime", ZonedDateTime.now())
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

            IonToAvro writer = IonToAvro.builder()
                .id(IonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .schema(schema)
                .build();
            writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
        }
    }

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
            // Rows 101–110: date field is non-null
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

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(null)
            .inferAllFields(Property.ofValue(true))
            .build();

        // Must succeed: all rows are scanned so event_date is typed correctly (not NULL)
        IonToAvro.Output output = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(110));
    }

    @Test
    void inferenceFailsOnEmptyFile() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_empty_", ".ion");
        // Write nothing to the file - it's empty

        URI uri;
        try (InputStream inputStream = new FileInputStream(tempFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), inputStream);
        }

        IonToAvro writer = IonToAvro.builder()
            .id(IonToAvro.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(null) // No schema - inference required
            .build();

        IllegalStateException exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );
        assertThat(exception.getMessage(), is("Cannot infer Avro schema from ION input: the file appears to be empty or contains no valid records."));
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

    private URI uploadRowsWithOneBadId() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_onbadlines_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.of("id", 1, "s", "a"),
                new HashMap<String, Object>() {{ put("id", null); put("s", "b"); }},
                ImmutableMap.of("id", 3, "s", "c")
            ).forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }

        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    // Permanent regression test (plan task 9): a null value into a non-nullable field must not make
    // IonToAvro fail under WARN. This is the reference behaviour the IonToParquet fix must match.
    @Test
    void onBadLinesWarnSkipsBadRowAndSucceeds() throws Exception {
        URI uri = uploadRowsWithOneBadId();

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        IonToAvro.Output output = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(output.getSize(), is(2L));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(2));
    }

    @Test
    void onBadLinesSkipDropsBadRowAndSucceeds() throws Exception {
        URI uri = uploadRowsWithOneBadId();

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NON_NULLABLE_INT_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.SKIP))
            .build();

        IonToAvro.Output output = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(output.getSize(), is(2L));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(2));
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

    private URI uploadIonRows(List<Map<String, Object>> rows) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_onbadlines_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            rows.forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    // Regression for the gate missing records nested inside an ARRAY field: same gap as
    // IonToParquetTest#warnSkipsBadRowInArrayOfRecordsField, exercised through IonToAvro. Must fail against the
    // pre-fix gate, which only checked `instanceof GenericData.Record` on direct field values.
    @Test
    void warnSkipsBadRowInArrayOfRecordsField() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "items", List.of(Map.of("score", 10), Map.of("score", 20))));
        Map<String, Object> badItem = new HashMap<>();
        badItem.put("score", null); // null into the non-nullable "score" field of an array element
        rows.add(Map.of("id", 2, "items", List.of(Map.of("score", 30), badItem)));
        rows.add(Map.of("id", 3, "items", List.of(Map.of("score", 40))));

        URI uri = uploadIonRows(rows);

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(ARRAY_OF_RECORDS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToAvro.Output output = writer.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(2));
        assertThat(
            listAppender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("items[1].score")),
            is(true)
        );
    }

    // Same gap as above but for a MAP field: see IonToParquetTest#warnSkipsBadRowInMapOfRecordsField.
    @Test
    void warnSkipsBadRowInMapOfRecordsField() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "byKey", Map.of("a", Map.of("score", 100))));
        Map<String, Object> badEntry = new HashMap<>();
        badEntry.put("score", null); // null into the non-nullable "score" field of a map value
        rows.add(Map.of("id", 2, "byKey", Map.of("a", badEntry)));
        rows.add(Map.of("id", 3, "byKey", Map.of("a", Map.of("score", 200))));

        URI uri = uploadIonRows(rows);

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(MAP_OF_RECORDS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToAvro.Output output = writer.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(2));
        assertThat(
            listAppender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("byKey{'a'}.score")),
            is(true)
        );
    }

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

    // Same regression as IonToParquetTest#warnKeepsAllRowsWhenSchemaHasLogicalTypesAndDataIsValid: the
    // GenericData.validate() gate used to drop every row of a logical-typed schema, even fully valid data.
    @Test
    void warnKeepsAllRowsWhenSchemaHasLogicalTypesAndDataIsValid() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_logical_", ".ion");
        List<Map<String, Object>> rows = IntStream.rangeClosed(1, 5).mapToObj(IonToAvroTest::logicalRow).toList();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            rows.forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(LOGICAL_TYPES_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .timeZoneId(Property.ofValue("UTC"))
            .build();

        IonToAvro.Output output = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(output.getSize(), is((long) rows.size()));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(rows.size()));
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

    // Counterpart to the logical-type gate regression, for container fields: see
    // IonToParquetTest#warnKeepsAllRowsWhenArrayAndMapOfRecordsAreValidWithLogicalSubfields.
    @Test
    void warnKeepsAllRowsWhenArrayAndMapOfRecordsAreValidWithLogicalSubfields() throws Exception {
        List<Map<String, Object>> rows = IntStream.rangeClosed(1, 5).mapToObj(IonToAvroTest::validNestedLogicalRow).toList();
        URI uri = uploadIonRows(rows);

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(uri.toString()))
            .schema(NESTED_LOGICAL_CONTAINERS_SCHEMA)
            .onBadLines(Property.ofValue(OnBadLines.WARN))
            .timeZoneId(Property.ofValue("UTC"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of());
        ListAppender<ILoggingEvent> listAppender = attachLogCapture(runContext);

        IonToAvro.Output output = writer.run(runContext);

        assertThat(output.getSize(), is((long) rows.size()));
        assertThat(avroSize(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())), is(rows.size()));
        assertThat(listAppender.list.isEmpty(), is(true));
    }
}
