package io.kestra.plugin.serdes.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.*;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@MicronautTest
public class AvroReaderWriterTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void basic() throws Exception {
        test("csv/insurance_sample.avro");
    }

    private void test(String file) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);


        AvroReader reader = AvroReader.builder()
            .id(AvroReaderTest.class.getSimpleName())
            .type(AvroReader.class.getName())
            .from(source.toString())
            .build();

        AvroReader.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        AvroWriter writer = AvroWriter.builder()
            .id(AvroWriterTest.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .schema(
                Files.asCharSource(
                    new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader().getResource(file.replace("avro","avsc"))).toURI()),
                    Charsets.UTF_8
                ).read()
            )
            .build();

        AvroWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        byte[] out = IOUtils.toByteArray(storageInterface.get(writerRunOutput.getUri()));
        File resultTestFile = SerdesUtils.resourceToFile(file);

        assertThat(out,is(IOUtils.toByteArray(new FileInputStream(resultTestFile)))
        );
    }


}
