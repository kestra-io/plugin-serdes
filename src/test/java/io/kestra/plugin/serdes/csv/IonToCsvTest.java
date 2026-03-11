package io.kestra.plugin.serdes.csv;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

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

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class IonToCsvTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    void map() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
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

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .fieldSeparator(Property.ofValue(";".charAt(0)))
                .alwaysDelimitText(Property.ofValue(false))
                .header(Property.ofValue(true))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            assertThat(out, containsString("string;int"));
            assertThat(out, containsString("3.2;" + ZonedDateTime.now().getYear()));
            assertThat(out, containsString("3.4;" + ZonedDateTime.now().getYear()));
        }
    }

    @Test
    void list() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
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

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .fieldSeparator(Property.ofValue(";".charAt(0)))
                .alwaysDelimitText(Property.ofValue(true))
                .header(Property.ofValue(false))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            assertThat(out, containsString("\"3.2\";\"" + ZonedDateTime.now().getYear()));
            assertThat(out, containsString("\"3.4\";\"" + ZonedDateTime.now().getYear()));
        }
    }

    @Test
    void alwaysDelimitTextFalse_mixedTypes() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("referencia", "PE1617279780")
                    .put("descripcion", "E:4 PAST FR")
                    .put("cantidad", 1)
                    .put("precio", 34.03)
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .header(Property.ofValue(false))
                .alwaysDelimitText(Property.ofValue(false))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            // With alwaysDelimitText=false, plain strings and numbers should NOT be quoted
            assertThat(out, is("PE1617279780,E:4 PAST FR,1,34.03\n"));
        }
    }

    @Test
    void quoteModeNonNumeric() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("referencia", "PE1617279780")
                    .put("descripcion", "E:4 PAST FR")
                    .put("cantidad", 1)
                    .put("precio", 34.03)
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .header(Property.ofValue(false))
                .quoteMode(Property.ofValue(IonToCsv.QuoteMode.NON_NUMERIC))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            // NON_NUMERIC: strings are quoted, numbers are not
            assertThat(out, is("\"PE1617279780\",\"E:4 PAST FR\",1,34.03\n"));
        }
    }

    @Test
    void quoteModeNonNumeric_withHeader() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("referencia", "PE1617279780")
                    .put("descripcion", "E:4 PAST FR")
                    .put("cantidad", 1)
                    .put("precio", 34.03)
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .header(Property.ofValue(true))
                .quoteMode(Property.ofValue(IonToCsv.QuoteMode.NON_NUMERIC))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            // Headers are also quoted (they are strings), data numbers are not
            assertThat(out, is("\"referencia\",\"descripcion\",\"cantidad\",\"precio\"\n\"PE1617279780\",\"E:4 PAST FR\",1,34.03\n"));
        }
    }

    @Test
    void quoteModeNonNumeric_negativeAndDecimal() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("label", "test")
                    .put("positive_int", 42)
                    .put("negative_int", -7)
                    .put("decimal", 3.14)
                    .put("negative_decimal", -0.5)
                    .put("not_a_number", "12abc")
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .header(Property.ofValue(false))
                .quoteMode(Property.ofValue(IonToCsv.QuoteMode.NON_NUMERIC))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            // Strings and non-numeric values are quoted, all numeric formats are not
            assertThat(out, is("\"test\",42,-7,3.14,-0.5,\"12abc\"\n"));
        }
    }

    @Test
    void alwaysDelimitTextFalse_preQuotedStrings() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                ImmutableMap.builder()
                    .put("referencia", "\"PE1617279780\"")
                    .put("descripcion", "\"E:4 PAST FR\"")
                    .put("cantidad", 1)
                    .put("precio", 34.03)
                    .build()
            )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToCsv writer = IonToCsv.builder()
                .id(IonToCsvTest.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .header(Property.ofValue(false))
                .alwaysDelimitText(Property.ofValue(false))
                .build();
            IonToCsv.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            String out = CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, writerRunOutput.getUri())));

            // Pre-embedded quotes get escaped per RFC 4180: " becomes "" and field is quoted
            // This is expected CSV behavior - the user's workaround of pre-quoting makes it worse
            assertThat(out, is("\"\"\"PE1617279780\"\"\",\"\"\"E:4 PAST FR\"\"\",1,34.03\n"));
        }
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

            IonToCsv writer = IonToCsv.builder()
                .id(IonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.ofValue(uri.toString()))
                .alwaysDelimitText(Property.ofValue(true))
                .header(Property.ofValue(false))
                .timeZoneId(Property.ofValue(ZoneId.of("Europe/Lisbon").toString()))
                .build();

            IonToCsv.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            assertThat(
                IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri()), Charsets.UTF_8),
                is(
                    "\"string\"," +
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
