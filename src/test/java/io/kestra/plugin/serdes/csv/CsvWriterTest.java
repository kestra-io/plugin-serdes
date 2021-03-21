package io.kestra.plugin.serdes.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static io.kestra.core.utils.Rethrow.throwConsumer;

@MicronautTest
class CsvWriterTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    void map() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try(OutputStream output = new FileOutputStream(tempFile)) {
            Arrays
                .asList(
                    ImmutableMap.builder()
                        .put("string", "string")
                        .put("int", 2)
                        .put("float", 3.2F)
                        .put("double", 3.2D)
                        .put("instant", Instant.now())
                        .put("zoned", ZonedDateTime.now())
                        .build(),
                    ImmutableMap.builder()
                        .put("string", "string")
                        .put("int", 2)
                        .put("float", 3.2F)
                        .put("double", 3.4D)
                        .put("instant", Instant.now())
                        .put("zoned", ZonedDateTime.now())
                        .build()
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put( URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            CsvWriter writer = CsvWriter.builder()
                .id(CsvWriterTest.class.getSimpleName())
                .type(CsvWriter.class.getName())
                .from(uri.toString())
                .fieldSeparator(";".charAt(0))
                .alwaysDelimitText(true)
                .header(true)
                .build();
            CsvWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri())));

            assertThat(out, containsString("\"string\";\"int\""));
            assertThat(out, containsString("\"3.2\";\"" + ZonedDateTime.now().getYear()));
            assertThat(out, containsString("\"3.4\";\"" + ZonedDateTime.now().getYear()));
        }
    }

    @Test
    void list() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try(OutputStream output = new FileOutputStream(tempFile)) {
            Arrays
                .asList(
                    Arrays.asList(
                         "string",
                         2,
                         3.2F,
                         3.2D,
                         Instant.now(),
                         ZonedDateTime.now()
                    ),
                    Arrays.asList(
                        "string",
                        2,
                        3.2F,
                        3.4D,
                        Instant.now(),
                        ZonedDateTime.now()
                    )
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put( URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            CsvWriter writer = CsvWriter.builder()
                .id(CsvWriterTest.class.getSimpleName())
                .type(CsvWriter.class.getName())
                .from(uri.toString())
                .fieldSeparator(";".charAt(0))
                .alwaysDelimitText(true)
                .header(false)
                .build();
            CsvWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri())));

            assertThat(out, containsString("\"3.2\";\"" + ZonedDateTime.now().getYear()));
            assertThat(out, containsString("\"3.4\";\"" + ZonedDateTime.now().getYear()));
        }
    }
}
