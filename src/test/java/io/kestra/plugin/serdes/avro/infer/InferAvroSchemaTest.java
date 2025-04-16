package io.kestra.plugin.serdes.avro.infer;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon;
import io.kestra.plugin.serdes.csv.IonToCsv;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.*;
import java.util.Date;
import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@KestraTest
public class InferAvroSchemaTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void ion() throws Exception {
        var tempFile = File.createTempFile(InferAvroSchemaTest.class.getSimpleName().toLowerCase() + "_", ".ion");
        try (var output = new FileOutputStream(tempFile)) {
            List.of(
                    ImmutableMap.builder()
                        .put("myString", "string")
                        .put("myInt", 2)
                        .put("myFloat", 3.2F)
                        .put("myDouble", 3.2D)// note there is no double in Ion, don't know why we parsed that
                        .put("myInstant", Instant.now())
                        .put("myZonedDateTime", ZonedDateTime.now())
                        .put("myLocalDateTime", LocalDateTime.now())
                        .put("myOffsetDateTime", OffsetDateTime.now())
                        .put("myLocalDate", LocalDate.now())
                        .put("myLocalTime", LocalTime.now())
                        .put("myOffsetTime", OffsetTime.now())
                        .put("myDate", new Date())
                        .build()
                )
                .forEach(throwConsumer(row -> FileSerde.write(output, row)));

            var inputIonFileUri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

            InferAvroSchemaFromIon writer = InferAvroSchemaFromIon.builder()
                .id(InferAvroSchemaFromIon.class.getSimpleName())
                .type(IonToCsv.class.getName())
                .from(Property.of(inputIonFileUri.toString()))
                .build();
            var run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

            // then
            var resultingSchemaStr = IOUtils.toString(new InputStreamReader(this.storageInterface.get(null, null, run.getUri())));
            JSONAssert.assertEquals("""
                    {
                      "type": "record",
                      "name": "root",
                      "namespace": "io.kestra.plugin.serdes.avro",
                      "fields": [
                        {
                          "name": "myString",
                          "type": ["null", "string"]
                        },
                        {
                          "name": "myInt",
                          "type": ["null", "int"]
                        },
                        {
                          "name": "myFloat",
                          "type": ["null", "double"]
                        },
                        {
                          "name": "myDouble",
                          "type": ["null", "double"]
                        },
                        {
                          "name": "myInstant",
                          "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
                        },
                        {
                          "name": "myZonedDateTime",
                          "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
                        },
                        {
                          "name": "myLocalDateTime",
                          "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
                        },
                        {
                          "name": "myOffsetDateTime",
                          "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
                        },
                        {
                          "name": "myLocalDate",
                          "type": ["null", {"type": "int", "logicalType": "date"}]
                        },
                        {
                          "name": "myLocalTime",
                          "type": ["null", {"type": "int", "logicalType": "time-millis"}]
                        },
                        {
                          "name": "myOffsetTime",
                          "type": ["null", {"type": "int", "logicalType": "time-millis"}]
                        },
                        {
                          "name": "myDate",
                          "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
                        }
                      ]
                    }
                    """,
                resultingSchemaStr,
                false
            );
        }

    }
}
