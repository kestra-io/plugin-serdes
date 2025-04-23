package io.kestra.plugin.serdes.parquet;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@KestraTest
class ParquetToIonWriterTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void ionWithInputSchema() throws Exception {
        testWithSchema(IOUtils.toString(
            Objects.requireNonNull(ParquetToIonWriterTest.class.getClassLoader().getResource("avro/all.avsc")),
            StandardCharsets.UTF_8
        ));
    }

    @Test
    void ionWithInferredSchema() throws Exception {
        testWithSchema(null);
    }

    @SuppressWarnings("unchecked")
    void testWithSchema(String schema) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                    ImmutableMap.builder()
                        .put("String", "string")
                        .put("Int", 2)
                        .put("Float", 3.2F)
                        .put("Double", 3.2D)
                        .put("Instant", Instant.now())
                        .put("ZonedDateTime", ZonedDateTime.parse("2021-08-02T12:00:00+02:00"))
                        .put("LocalDateTime", LocalDateTime.now())
                        .put("OffsetDateTime", OffsetDateTime.now())
                        .put("LocalDate", LocalDate.now())
                        .put("LocalTime", LocalTime.now())
                        .put("OffsetTime", OffsetTime.now())
                        .put("Date", new Date())
                        .build()
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            IonToParquet writer = IonToParquet.builder()
                .id(IonToParquet.class.getSimpleName())
                .type(IonToParquet.class.getName())
                .from(Property.of(uri.toString()))
                .schema(schema)
                .timeZoneId(Property.of("UTC"))
                .build();

            IonToParquet.Output writerOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            ParquetToIon reader = ParquetToIon.builder()
                .id(ParquetToIon.class.getSimpleName())
                .type(ParquetToIon.class.getName())
                .from(Property.of(writerOutput.getUri().toString()))
                .build();

            ParquetToIon.Output readerOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

            List<Map<String, Object>> result = new ArrayList<>();
            FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, readerOutput.getUri()))), r -> result.add((Map<String, Object>) r));

            assertThat(result.size(), is(1));
            assertThat(result.get(0).get("String"), is("string"));
            assertThat(result.get(0).get("ZonedDateTime"), instanceOf(LocalDateTime.class));
            assertThat(result.get(0).get("ZonedDateTime"), is(LocalDateTime.parse("2021-08-02T10:00:00")));
        }
    }
}
