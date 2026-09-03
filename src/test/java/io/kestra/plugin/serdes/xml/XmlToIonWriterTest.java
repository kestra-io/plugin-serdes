package io.kestra.plugin.serdes.xml;

import java.io.*;
import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.avro.IonToAvro;
import io.kestra.plugin.serdes.csv.IonToCsv;
import io.kestra.plugin.serdes.json.IonToJson;

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
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
            .query(Property.ofValue(query))
            .from(Property.ofValue(source.toString()))
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private IonToXml.Output writer(URI from) throws Exception {
        IonToXml writer = IonToXml.builder()
            .id(IonToJson.class.getSimpleName())
            .type(IonToJson.class.getName())
            .from(Property.ofValue(from.toString()))
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
            IOUtils.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile), Charsets.UTF_8))
        );
        assertThat(readerRunOutput.getSize(), is(greaterThan(0L)));
        assertThat(writerRunOutput.getSize(), is(greaterThan(0L)));
    }

    @Test
    void docbook() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/docbook.xml");
        File resultFile = SerdesUtils.resourceToFile("xml/docbook_result.xml");

        XmlToIon.Output readerRunOutput = this.reader(sourceFile, null);
        IonToXml.Output writerRunOutput = this.writer(readerRunOutput.getUri());

        assertThat(
            IOUtils.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile), Charsets.UTF_8))
        );
    }

    @Test
    void ion() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
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

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToXml writer = IonToXml.builder()
                .id(IonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .timeZoneId(Property.ofValue(ZoneId.of("Europe/Lisbon").toString()))
                .build();

            IonToXml.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri()), Charsets.UTF_8),
                is(
                    "<?xml version='1.0' encoding='UTF-8'?>\n<items>\n  <item>\n    " +
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
            assertThat(run.getSize(), is(1L));
        }
    }

    @Test
    void readItemsListUnwrapsToIndividualRecords() throws Exception {
        // Without a query, a root wrapping a single repeated complex child element (the
        // pattern produced by IonToXml) unwraps into individual ION records — and this must
        // hold regardless of how many occurrences there are (see #371).
        File sourceFile = SerdesUtils.resourceToFile("xml/items.xml");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        // Should produce 3 individual records, not 1 nested record
        assertThat(records.size(), is(3));

        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) records.getFirst();
        assertThat(first.get("job_title"), is("BI Data Analyst"));
    }

    @Test
    void singleBookStillUnwrapsToOneRecord() throws Exception {
        // A single occurrence of the repeated child must take the same unwrap path as
        // several occurrences, so the output shape does not depend on record count (#371).
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xml");
        java.nio.file.Files.writeString(sourceFile.toPath(), "<catalog><book><id>r1</id></book></catalog>");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        assertThat(records.size(), is(1));

        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) records.getFirst();
        assertThat(first.get("id"), is("r1"));
    }

    @Test
    void twoBooksProduceSameShapeAsOneBook() throws Exception {
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xml");
        java.nio.file.Files.writeString(sourceFile.toPath(), "<catalog><book><id>r1</id></book><book><id>r2</id></book></catalog>");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        assertThat(records.size(), is(2));

        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) records.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> second = (Map<String, Object>) records.get(1);
        assertThat(first.get("id"), is("r1"));
        assertThat(second.get("id"), is("r2"));
        // Same flat shape as the single-book case: only an "id" key, no "catalog"/"book" nesting.
        assertThat(first.keySet(), is(second.keySet()));
        assertThat(first.keySet(), is(java.util.Set.of("id")));
    }

    @Test
    void multipleDistinctChildrenStaySingleRecord() throws Exception {
        // A root with more than one distinct child element name is not a record collection.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xml");
        java.nio.file.Files.writeString(sourceFile.toPath(), "<root><book><id>r1</id></book><author><id>a1</id></author></root>");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        assertThat(records.size(), is(1));

        @SuppressWarnings("unchecked")
        Map<String, Object> record = (Map<String, Object>) records.getFirst();
        assertThat(record.containsKey("root"), is(true));
    }

    @Test
    void scalarChildStaysSingleRecord() throws Exception {
        // <root><value>5</value></root>: "value" is a scalar leaf (no attributes, no nested
        // elements), so it must not be mistaken for a one-record collection.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xml");
        java.nio.file.Files.writeString(sourceFile.toPath(), "<root><value>5</value></root>");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        assertThat(records.size(), is(1));

        @SuppressWarnings("unchecked")
        Map<String, Object> record = (Map<String, Object>) records.getFirst();
        assertThat(record.containsKey("root"), is(true));
    }

    @Test
    void unwrapRootCollectionFalseKeepsSingleNestedRecord() throws Exception {
        // The escape hatch for genuinely ambiguous, config-shaped XML.
        File sourceFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".xml");
        java.nio.file.Files.writeString(sourceFile.toPath(), "<catalog><book><id>r1</id></book><book><id>r2</id></book></catalog>");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlToIon reader = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .unwrapRootCollection(Property.ofValue(false))
            .build();

        XmlToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));

        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        assertThat(records.size(), is(1));

        @SuppressWarnings("unchecked")
        Map<String, Object> record = (Map<String, Object>) records.getFirst();
        assertThat(record.containsKey("catalog"), is(true));
    }

    @Test
    void roundTripIonToXmlAndBack() throws Exception {
        // Write ION records to XML, then read back and verify we get the original records,
        // without setting `query` (this is the whole point of #86).
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.of("job_title", "Data Engineer", "avg_salary", 157510.03),
                ImmutableMap.of("job_title", "Data Analyst", "avg_salary", 116348.29)
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }

        URI ionUri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        // Write to XML
        IonToXml.Output writerOutput = this.writer(ionUri);

        // Read back from XML
        XmlToIon readerTask = XmlToIon.builder()
            .id(XmlToIon.class.getSimpleName())
            .type(XmlToIon.class.getName())
            .from(Property.ofValue(writerOutput.getUri().toString()))
            .build();

        XmlToIon.Output readerOutput = readerTask.run(TestsUtils.mockRunContext(this.runContextFactory, readerTask, ImmutableMap.of()));

        // Read ION records
        var records = new java.util.ArrayList<>();
        try (
            var inputStream = runContextFactory.of().storage().getFile(readerOutput.getUri())
        ) {
            FileSerde.readAll(inputStream).collectList().block().forEach(records::add);
        }

        // Should get 2 individual records back
        assertThat(records.size(), is(2));

        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) records.getFirst();
        assertThat(first.get("job_title"), is("Data Engineer"));
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmpty() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/random/stuff");
        List<Object> records;
        try (var inputStream = runContextFactory.of().storage().getFile(reader.getUri())) {
            records = FileSerde.readAll(inputStream).collectList().block();
        }
        assertThat(records, is(empty()));
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmptyTagBadQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty-tag.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/random/stuff");
        List<Object> records;
        try (var inputStream = runContextFactory.of().storage().getFile(reader.getUri())) {
            records = FileSerde.readAll(inputStream).collectList().block();
        }
        assertThat(records, is(empty()));
    }

    @Test
    // Assert that there is no exception throw when reading an empty file
    void readEmptyTagGoodQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/empty-tag.xml");
        XmlToIon.Output reader = this.reader(sourceFile, "/catalog");
        List<Object> records;
        try (var inputStream = runContextFactory.of().storage().getFile(reader.getUri())) {
            records = FileSerde.readAll(inputStream).collectList().block();
        }
        assertThat(records, contains(""));
    }

    @Test
    void largeXmlStreaming() throws Exception {
        int recordCount = 10_000;

        // Generate a large XML file
        File largeXml = File.createTempFile("large_xml_", ".xml");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(largeXml))) {
            bw.write("<?xml version=\"1.0\"?>\n<catalog>\n");
            for (int i = 0; i < recordCount; i++) {
                bw.write("  <item id=\"" + i + "\"><name>Item " + i + "</name><value>" + (i * 1.5) + "</value></item>\n");
            }
            bw.write("</catalog>\n");
        }

        XmlToIon.Output output = this.reader(largeXml, "/catalog/item");

        // Read back all records and count them. Output is binary ION, so decode it via
        // FileSerde instead of counting text lines, which assumes a text ION format.
        int count;
        try (var inputStream = runContextFactory.of().storage().getFile(output.getUri())) {
            count = FileSerde.readAll(inputStream).collectList().block().size();
        }

        assertThat(count, is(recordCount));
        largeXml.delete();
    }
}
