package io.kestra.plugin.serdes;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.avro.InferIonToAvro;
import io.kestra.plugin.serdes.csv.IonToCsv;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
public class InferIonToAvroTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void ion() throws Exception {
        File tempFile = File.createTempFile(InferIonToAvroTest.class.getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
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

            URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            InferIonToAvro writer = InferIonToAvro.builder()
                .id(InferIonToAvro.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.of(uri.toString()))
                .build();
            var run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            // then
            var schemaParser = new Schema.Parser();
            var expectedSchema = schemaParser.parse(
                IOUtils.toString(Objects.requireNonNull(InferIonToAvroTest.class.getClassLoader().getResourceAsStream("avro/all.avsc")), StandardCharsets.UTF_8)
            );
            var resultingSchemaStr = IOUtils.toString(new InputStreamReader(this.storageInterface.get(null, null, run.getUri())));
            var resultingSchema = schemaParser.parse(resultingSchemaStr);

            assertThat(resultingSchema.getFields()).isEqualTo(expectedSchema.getFields());
            assertThat(resultingSchema).isEqualTo(expectedSchema);
        }

    }
}
