package io.kestra.plugin.serdes.csv;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.avro.AvroWriter;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import javax.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

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
                        .put("Instant", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toInstant())
                        .put("ZonedDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00"))
                        .put("LocalDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalDateTime().truncatedTo(ChronoUnit.MINUTES))
                        .put("OffsetDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toOffsetDateTime())
                        .put("LocalDate", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalDate())
                        .put("LocalTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalTime())
                        .put("OffsetTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toOffsetDateTime().toOffsetTime())
                        .put("Date", Date.from(ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toInstant()))
                        .build()
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            CsvWriter writer = CsvWriter.builder()
                .id(AvroWriter.class.getSimpleName())
                .type(CsvWriter.class.getName())
                .from(uri.toString())
                .alwaysDelimitText(true)
                .header(false)
                .timeZoneId(ZoneId.of("Europe/Lisbon").toString())
                .build();

            CsvWriter.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(run.getUri()), Charsets.UTF_8),
                is("\"string\"," +
                    "\"2\"," +
                    "\"3.200000047683716\"," +
                    "\"3.2\"," +
                    "\"2021-05-05T10:21:12.123Z\"," +
                    "\"2021-05-05T11:21:12.123+01:00\"," +
                    "\"2021-05-05T12:21:00.000\"," +
                    "\"2021-05-05T11:21:12.123+01:00\"," +
                    "\"2021-05-05\"," +
                    "\"12:21:12\"," +
                    "\"12:21:12+02:00\"," +
                    "\"2021-05-05T10:21:12.123Z\"" +
                    "\n"
                )
            );
        }
    }
}
