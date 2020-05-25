package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.serdes.SerdesUtils;
import org.kestra.task.serdes.avro.AvroWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class JsonReaderWriterTest {
    @Inject
    ApplicationContext applicationContext;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    private void run(String fileSource, boolean jsonNl) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(fileSource);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonReader reader = JsonReader.builder()
            .id(JsonReader.class.getSimpleName())
            .type(JsonReader.class.getName())
            .from(source.toString())
            .newLine(jsonNl)
            .build();
        JsonReader.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(this.applicationContext, reader, ImmutableMap.of()));

        JsonWriter writer = JsonWriter.builder()
            .id(JsonWriter.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .newLine(jsonNl)
            .build();
        JsonWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(applicationContext, writer, ImmutableMap.of()));

        ObjectMapper mapper = new ObjectMapper();

        assertThat(
            mapper.readTree(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(mapper.readTree(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void newLine() throws Exception {
        this.run("csv/full.jsonl", true);
    }

    @Test
    void array() throws Exception {
        this.run("csv/full.json", false);
    }
}
