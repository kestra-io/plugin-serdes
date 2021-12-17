package io.kestra.plugin.serdes.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import java.io.*;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@MicronautTest
public class AvroReaderTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void basic() throws Exception {
        test("csv/insurance_sample.avro");
    }

    private void test(String file) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);


        AvroReader task = AvroReader.builder()
            .id(AvroReaderTest.class.getSimpleName())
            .type(AvroReader.class.getName())
            .from(source.toString())
            .build();

        AvroReader.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        File resultTestFile = SerdesUtils.resourceToFile(file.replace("avro","ion"));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(run.getUri()))),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(resultTestFile))))
        );
    }


}
