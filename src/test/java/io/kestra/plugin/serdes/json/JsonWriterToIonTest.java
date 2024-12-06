package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.avro.IonToAvro;
import io.kestra.plugin.serdes.csv.IonToCsv;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class JsonWriterToIonTest {
    private static ObjectMapper mapper = new ObjectMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private JsonToIon.Output reader(File sourceFile, boolean jsonNl) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(JsonToIon.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.of(source.toString()))
            .newLine(Property.of(jsonNl))
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private IonToJson.Output writer(URI from, boolean jsonNl) throws Exception {
        IonToJson writer = IonToJson.builder()
            .id(IonToJson.class.getSimpleName())
            .type(IonToJson.class.getName())
            .from(Property.of(from.toString()))
            .newLine(Property.of(jsonNl))
            .build();

        return writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
    }

    @Test
    void newLine() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, true);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), true);

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(null, null, writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
        assertThat(
            FilenameUtils.getExtension(writerRunOutput.getUri().getPath()),
            is("jsonl")
        );
    }

    @Test
    void notNewLine() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, true);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);

        assertThat(
            FilenameUtils.getExtension(writerRunOutput.getUri().getPath()),
            is("json")
        );
    }

    @Test
    void array() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.json");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, false);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(null, null, writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void object() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/object.json");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, false);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);


        List<Map> objects = Arrays.asList(mapper.readValue(
            new InputStreamReader(storageInterface.get(null, null, writerRunOutput.getUri())),
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
                        .put("Instant", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toInstant())
                        .put("ZonedDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00"))
                        .put("LocalDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalDateTime())
                        .put("OffsetDateTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toOffsetDateTime())
                        .put("LocalDate", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalDate())
                        .put("LocalTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toLocalTime())
                        .put("OffsetTime", ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toOffsetDateTime().toOffsetTime())
                        .put("Date", Date.from(ZonedDateTime.parse("2021-05-05T12:21:12.123456+02:00").toInstant()))
                        .build()
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToJson writer = IonToJson.builder()
                .id(IonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.of(uri.toString()))
                .timeZoneId(Property.of(ZoneId.of("Europe/Lisbon").toString()))
                .build();
            IonToJson.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(null, null, run.getUri()), Charsets.UTF_8),
                is("{" +
                    "\"String\":\"string\"," +
                    "\"Int\":2," +
                    "\"Float\":3.200000047683716," +
                    "\"Double\":3.2," +
                    "\"Instant\":\"2021-05-05T10:21:12.123Z\"," +
                    "\"ZonedDateTime\":\"2021-05-05T11:21:12.123456+01:00\"," +
                    "\"LocalDateTime\":\"2021-05-05T12:21:12.123456\"," +
                    "\"OffsetDateTime\":\"2021-05-05T11:21:12.123456+01:00\"," +
                    "\"LocalDate\":\"2021-05-05\"," +
                    "\"LocalTime\":\"12:21:12.123456\"," +
                    "\"OffsetTime\":\"12:21:12.123456+02:00\"," +
                    "\"Date\":\"2021-05-05T10:21:12.123Z\"" +
                    "}\n"
                )
            );
        }
    }
}
