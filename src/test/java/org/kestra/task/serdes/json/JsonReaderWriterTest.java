package org.kestra.task.serdes.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.serdes.SerdesUtils;
import org.kestra.task.serdes.avro.AvroWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

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
            .type(JsonReader.class.getName())
            .from(source.toString())
            .newLine(jsonNl)
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private JsonWriter.Output writer(URI from, boolean jsonNl) throws Exception {
        JsonWriter writer = JsonWriter.builder()
            .id(JsonWriter.class.getSimpleName())
            .type(AvroWriter.class.getName())
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
}
