package io.kestra.plugin.serdes.avro;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

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
}
