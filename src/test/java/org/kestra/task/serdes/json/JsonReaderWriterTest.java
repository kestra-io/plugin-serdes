package org.kestra.task.serdes.json;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
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
    void run() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonReader reader = JsonReader.builder()
            .from(source.toString())
            .build();
        JsonReader.Output readerRunOutput = reader.run(new RunContext(this.applicationContext, ImmutableMap.of()));

        JsonWriter writer = JsonWriter.builder()
            .id(JsonWriter.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .build();
        JsonWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(applicationContext, writer, ImmutableMap.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }
}
