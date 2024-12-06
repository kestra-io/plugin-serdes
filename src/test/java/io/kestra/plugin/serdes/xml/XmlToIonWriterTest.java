package io.kestra.plugin.serdes.xml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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
import io.kestra.plugin.serdes.json.IonToJson;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class XmlToIonWriterTest {
    private static ObjectMapper mapper = new XmlMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private XmlToIon.Output reader(File sourceFile, String query) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .query(Property.of(query))
            .from(Property.of(source.toString()))
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private IonToXml.Output writer(URI from) throws Exception {
        IonToXml writer = IonToXml.builder()
            .id(IonToJson.class.getSimpleName())
            .type(IonToJson.class.getName())
            .from(Property.of(from.toString()))
            .build();

        return writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
    }

    @Test
    void bookWithQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/book.xml");
        File resultFile = SerdesUtils.resourceToFile("xml/book_result.xml");

        XmlToIon.Output readerRunOutput = this.reader(sourceFile, "/catalog/book");
        IonToXml.Output writerRunOutput = this.writer(readerRunOutput.getUri());

        assertThat(
            IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile), Charsets.UTF_8))
        );
    }

    @Test
    void docbook() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/docbook.xml");
        File resultFile = SerdesUtils.resourceToFile("xml/docbook_result.xml");

        XmlToIon.Output readerRunOutput = this.reader(sourceFile, null);
        IonToXml.Output writerRunOutput = this.writer(readerRunOutput.getUri());

        assertThat(
            IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile), Charsets.UTF_8))
        );
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

            URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToXml writer = IonToXml.builder()
                .id(IonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.of(uri.toString()))
                .timeZoneId(Property.of(ZoneId.of("Europe/Lisbon").toString()))
                .build();

            IonToXml.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(null, null, run.getUri()), Charsets.UTF_8),
                is("<?xml version='1.0' encoding='UTF-8'?>\n<items>\n  <item>\n    " +
                    "<String>string</String>\n    " +
                    "<Int>2</Int>\n    " +
                    "<Float>3.200000047683716</Float>\n    " +
                    "<Double>3.2</Double>\n    " +
                    "<Instant>2021-05-05T10:21:12.123Z</Instant>\n    " +
                    "<ZonedDateTime>2021-05-05T11:21:12.123456+01:00</ZonedDateTime>\n    " +
                    "<LocalDateTime>2021-05-05T12:21:00</LocalDateTime>\n    " +
                    "<OffsetDateTime>2021-05-05T11:21:12.123456+01:00</OffsetDateTime>\n    " +
                    "<LocalDate>2021-05-05</LocalDate>\n    " +
                    "<LocalTime>12:21:12.123456</LocalTime>\n    " +
                    "<OffsetTime>12:21:12.123456+02:00</OffsetTime>\n    " +
                    "<Date>2021-05-05T10:21:12.123Z</Date>\n  " +
                    "</item>\n</items>\n"
                )
            );
        }
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmpty() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/random/stuff");
        String tagContent = new BufferedReader(new
            InputStreamReader(runContextFactory.of().storage().getFile(reader.getUri()))).lines().collect(Collectors.joining("\n"));
        assertThat(tagContent, is(""));
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmptyTagBadQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty-tag.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/random/stuff");
        String tagContent = new BufferedReader(new
            InputStreamReader(runContextFactory.of().storage().getFile(reader.getUri()))).lines().collect(Collectors.joining("\n"));
        assertThat(tagContent, is(""));
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmptyTagGoodQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty-tag.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/catalog");
        String tagContent = new BufferedReader(new
            InputStreamReader(runContextFactory.of().storage().getFile(reader.getUri()))).lines().collect(Collectors.joining("\n"));
        assertThat(tagContent, is("\"\""));
    }
}
