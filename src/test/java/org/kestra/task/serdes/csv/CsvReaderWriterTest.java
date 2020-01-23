package org.kestra.task.serdes.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.commons.lang3.ArrayUtils;
import org.kestra.core.models.executions.AbstractMetricEntry;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.storages.StorageObject;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.serdes.SerdesUtils;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class CsvReaderWriterTest {
    @Inject
    ApplicationContext applicationContext;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private void test(String file, boolean header) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        StorageObject source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvReader reader = CsvReader.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvReader.class.getName())
            .from(source.getUri().toString())
            .fieldSeparator(";".charAt(0))
            .header(header)
            .build();
        RunOutput readerRunOutput = reader.run(TestsUtils.mockRunContext(applicationContext, reader, ImmutableMap.of()));

        CsvWriter writer = CsvWriter.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvWriter.class.getName())
            .from(readerRunOutput.getOutputs().get("uri").toString())
            .fieldSeparator(";".charAt(0))
            .alwaysDelimitText(true)
            .lineDelimiter(ArrayUtils.toObject((file.equals("csv/insurance_sample.csv") ? "\r\n" : "\n").toCharArray()))
            .header(header)
            .build();
        RunOutput writerRunOutput = writer.run(TestsUtils.mockRunContext(applicationContext, writer, ImmutableMap.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get((URI) writerRunOutput.getOutputs().get("uri")))),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(sourceFile))))
        );
    }

    @Test
    void header() throws Exception {
        this.test("csv/insurance_sample.csv", true);
    }

    @Test
    void noHeader() throws Exception {
        this.test("csv/insurance_sample_no_header.csv", false);
    }

    @Test
    void skipRows() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("csv/insurance_sample.csv");
        StorageObject source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvReader reader = CsvReader.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvReader.class.getName())
            .from(source.getUri().toString())
            .fieldSeparator(";".charAt(0))
            .skipRows(4)
            .header(false)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(applicationContext, reader, ImmutableMap.of());
        RunOutput readerRunOutput = reader.run(runContext);

        Counter records = (Counter) runContext.metrics()
            .stream()
            .filter(metricEntry -> metricEntry.getName().equals("records"))
            .findFirst()
            .get();

        assertThat(records.getValue(), is(2D));
    }

}