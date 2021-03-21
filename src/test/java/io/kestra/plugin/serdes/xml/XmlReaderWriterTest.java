package io.kestra.plugin.serdes.xml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.json.JsonWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class XmlReaderWriterTest {
    private static ObjectMapper mapper = new XmlMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private XmlReader.Output reader(File sourceFile, String query) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        XmlReader reader = XmlReader.builder()
            .id(XmlReader.class.getSimpleName())
            .type(XmlReader.class.getName())
            .query(query)
            .from(source.toString())
            .build();

        return reader.run(TestsUtils.mockRunContext(this.runContextFactory, reader, ImmutableMap.of()));
    }

    private XmlWriter.Output writer(URI from) throws Exception {
        XmlWriter writer = XmlWriter.builder()
            .id(JsonWriter.class.getSimpleName())
            .type(JsonWriter.class.getName())
            .from(from.toString())
            .build();

        return writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));
    }

    @Test
    void bookWithQuery() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/book.xml");
        File resultFile = SerdesUtils.resourceToFile("xml/book_result.xml");

        XmlReader.Output readerRunOutput = this.reader(sourceFile, "/catalog/book");
        XmlWriter.Output writerRunOutput = this.writer(readerRunOutput.getUri());

        assertThat(
            IOUtils.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile)))
        );
    }

    @Test
    void docbook() throws Exception {
        File sourceFile = SerdesUtils.resourceToFile("xml/docbook.xml");
        File resultFile = SerdesUtils.resourceToFile("xml/docbook_result.xml");

        XmlReader.Output readerRunOutput = this.reader(sourceFile, null);
        XmlWriter.Output writerRunOutput = this.writer(readerRunOutput.getUri());

        assertThat(
            IOUtils.toString(new InputStreamReader(storageInterface.get(writerRunOutput.getUri()))),
            is(IOUtils.toString(new FileInputStream(resultFile)))
        );
    }

}
