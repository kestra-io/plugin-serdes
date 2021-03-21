package io.kestra.plugin.serdes.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class CsvReaderWriterTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private void test(String file, boolean header) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvReader reader = CsvReader.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvReader.class.getName())
            .from(source.toString())
            .fieldSeparator(";".charAt(0))
            .header(header)
            .build();
        CsvReader.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        CsvWriter writer = CsvWriter.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .fieldSeparator(";".charAt(0))
            .alwaysDelimitText(true)
            .lineDelimiter(ArrayUtils.toObject((file.equals("csv/insurance_sample.csv") ? "\r\n" : "\n").toCharArray()))
            .header(header)
            .build();
        CsvWriter.Output writerRunOutput = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
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
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvReader reader = CsvReader.builder()
            .id(CsvReaderWriterTest.class.getSimpleName())
            .type(CsvReader.class.getName())
            .from(source.toString())
            .fieldSeparator(";".charAt(0))
            .skipRows(4)
            .header(false)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of());
        reader.run(runContext);

        Counter records = (Counter) runContext.metrics()
            .stream()
            .filter(metricEntry -> metricEntry.getName().equals("records"))
            .findFirst()
            .get();

        assertThat(records.getValue(), is(2D));
    }

}
