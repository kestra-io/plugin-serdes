package io.kestra.plugin.serdes.parquet;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
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

    /**
     * Verifies that inferAllFields=true scans all rows, so a date field that is null in the first 100
     * rows is not permanently typed as NULL when non-null values appear from row 101 onward.
     */
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

    /**
     * Verifies that inferAllFields=false (default) limits schema inference to numberOfRowsToScan rows,
     * causing a type conflict when a date field is null in those rows but non-null later.
     */
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
        assertThrows(
            Exception.class,
            () -> writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()))
        );
    }
}
