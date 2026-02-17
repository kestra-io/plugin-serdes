package io.kestra.plugin.serdes.avro;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.csv.IonToCsv;
import jakarta.inject.Inject;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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
            new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                    .getResource(file))
                .toURI()))
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

        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri())),
            is(IonToAvroTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                        .getResource("csv/insurance_sample.avro"))
                    .toURI())))
            )
        );
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
        runIonToAvroTestWithSchema(IOUtils.toString(
            Objects.requireNonNull(IonToAvroTest.class.getClassLoader().getResource("avro/all.avsc")),
            StandardCharsets.UTF_8
        ));
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
}
