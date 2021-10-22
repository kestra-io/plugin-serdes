package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.avro.AvroWriter;
import io.kestra.plugin.serdes.csv.CsvWriter;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class JsonReaderWriterTest {
    private static ObjectMapper mapper = new ObjectMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private JsonReader.Output reader(File sourceFile, boolean jsonNl) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonReader reader = JsonReader.builder()
            .id(JsonReader.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(source.toString())
            .newLine(jsonNl)
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private JsonWriter.Output writer(URI from, boolean jsonNl) throws Exception {
        JsonWriter writer = JsonWriter.builder()
            .id(JsonWriter.class.getSimpleName())
            .type(JsonWriter.class.getName())
            .from(from.toString())
            .newLine(jsonNl)
            .build();

        return writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
    }

    @Test
    void newLine() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");

        JsonReader.Output readerRunOutput = this.reader(sourceFile, true);
        JsonWriter.Output writerRunOutput = this.writer(readerRunOutput.getUri(), true);

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void array() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.json");

        JsonReader.Output readerRunOutput = this.reader(sourceFile, false);
        JsonWriter.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void object() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/object.json");

        JsonReader.Output readerRunOutput = this.reader(sourceFile, false);
        JsonWriter.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);


        List<Map> objects = Arrays.asList(mapper.readValue(
            new InputStreamReader(storageInterface.get(writerRunOutput.getUri())),
            Map[].class
        ));

        assertThat(objects.size(), is(1));
        assertThat(objects.get(0).get("id"), is(4814976));
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

            JsonWriter writer = JsonWriter.builder()
                .id(AvroWriter.class.getSimpleName())
                .type(CsvWriter.class.getName())
                .from(uri.toString())
                .build();
            JsonWriter.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(run.getUri()), Charsets.UTF_8),
                is("{\"String\":\"string\",\"Int\":2,\"Float\":3.200000047683716,\"Double\":3.2,\"Instant\":\"2021-10-22T17:17:51.828Z\",\"ZonedDateTime\":\"2021-10-22T19:17:51.832574+02:00\",\"LocalDateTime\":\"2021-10-22T19:17:51.833451\",\"OffsetDateTime\":\"2021-10-22T19:17:51.833556+02:00\",\"LocalDate\":\"2021-10-22\",\"LocalTime\":\"19:17:51.833618\",\"OffsetTime\":\"19:17:51.833722+02:00\",\"Date\":\"2021-10-22T17:17:51.833Z\"}")
            );
        }
    }
}
