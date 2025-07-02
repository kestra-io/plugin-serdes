package io.kestra.plugin.serdes.json;

import com.amazon.ion.IonType;
import com.amazon.ion.system.IonSystemBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.avro.IonToAvro;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

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
            .from(Property.ofValue(source.toString()))
            .newLine(Property.ofValue(jsonNl))
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private IonToJson.Output writer(URI from, boolean jsonNl) throws Exception {
        IonToJson writer = IonToJson.builder()
            .id(IonToJson.class.getSimpleName())
            .type(IonToJson.class.getName())
            .from(Property.ofValue(from.toString()))
            .newLine(Property.ofValue(jsonNl))
            .build();

        return writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
    }

    @Test
    void newLine() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, true);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), true);

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri()))),
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
            mapper.readTree(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void object() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/object.json");

        JsonToIon.Output readerRunOutput = this.reader(sourceFile, false);
        IonToJson.Output writerRunOutput = this.writer(readerRunOutput.getUri(), false);

        Map<String, Object> object = mapper.readValue(
            new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())),
            Map.class
        );

        assertThat(object.get("id"), is(4814976));
    }

    @Test
    void ion() throws Exception {
        var tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        var ionSystem = IonSystemBuilder.standard().build();

        var sourceDateTime = ZonedDateTime.of(2021, 5, 5, 12, 21, 12, 123_456_000, ZoneOffset.of("+02:00"));
        var instant = sourceDateTime.toInstant();
        var zoneId = ZoneId.of("Europe/Lisbon");
        var zonedDateTime = sourceDateTime.withZoneSameInstant(zoneId);
        var offset = zoneId.getRules().getOffset(instant);
        var offsetDateTime = instant.atOffset(offset);
        var truncatedInstant = instant.truncatedTo(ChronoUnit.MILLIS);
        var truncatedInstantStr = truncatedInstant.toString();

        try (var output = new FileOutputStream(tempFile);
             var writer = ionSystem.newTextWriter(output)) {

            writer.stepIn(IonType.STRUCT);

            writer.setFieldName("String");
            writer.writeString("string");

            writer.setFieldName("Int");
            writer.writeInt(2);

            writer.setFieldName("Float");
            writer.writeFloat(3.2f);

            writer.setFieldName("Double");
            writer.writeFloat(3.2d);

            writer.setFieldName("Instant");
            writer.writeString(truncatedInstantStr);

            writer.setFieldName("ZonedDateTime");
            writer.writeString(zonedDateTime.toOffsetDateTime().toString());

            writer.setFieldName("LocalDateTime");
            writer.writeString(sourceDateTime.toLocalDateTime().toString());

            writer.setFieldName("OffsetDateTime");
            writer.writeString(offsetDateTime.toString());

            writer.setFieldName("LocalDate");
            writer.writeString(sourceDateTime.toLocalDate().toString());

            writer.setFieldName("LocalTime");
            writer.writeString(sourceDateTime.toLocalTime().toString());

            writer.setFieldName("OffsetTime");
            writer.writeString(sourceDateTime.toOffsetDateTime().toOffsetTime().toString());

            writer.setFieldName("Date");
            writer.writeString(truncatedInstantStr);

            writer.stepOut();
        }

        var uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        var writer = IonToJson.builder()
            .id(IonToJson.class.getSimpleName())
            .type(IonToJson.class.getName())
            .from(Property.ofValue(uri.toString()))
            .timeZoneId(Property.ofValue(zoneId.toString()))
            .build();

        var run = writer.run(
            TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of())
        );

        var actual = IOUtils.toString(
            storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri()),
            Charsets.UTF_8
        );

        var expected = "{" +
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
            "}\n";

        assertThat(actual, is(expected));
    }
}
