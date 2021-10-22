package io.kestra.plugin.serdes.avro;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.csv.CsvWriter;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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
import javax.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class AvroWriterTest {
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
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader()
                .getResource(file))
                .toURI()))
        );

        AvroWriter task = AvroWriter.builder()
            .id(AvroWriterTest.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(source.toString())
            .schema(
                Files.asCharSource(
                    new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader().getResource("csv/insurance_sample.avsc")).toURI()),
                    Charsets.UTF_8
                ).read()
            )
            .build();

        AvroWriter.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
            AvroWriterTest.avroSize(this.storageInterface.get(run.getUri())),
            is(AvroWriterTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader()
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
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try(OutputStream output = new FileOutputStream(tempFile)) {
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

            URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            AvroWriter writer = AvroWriter.builder()
                .id(AvroWriter.class.getSimpleName())
                .type(CsvWriter.class.getName())
                .from(uri.toString())
                .schema(IOUtils.toString(
                    Objects.requireNonNull(AvroWriterTest.class.getClassLoader().getResource("avro/all.avsc")),
                    StandardCharsets.UTF_8
                ))
                .build();
            writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
        }
    }
}
