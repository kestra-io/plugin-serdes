package io.kestra.plugin.serdes.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@KestraTest
public class AvroToIonWriterTest {
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

    @Test
    void nullValues() throws Exception {
        test("avro/null.avro");
    }

    private void test(String file) throws Exception {
        File sourceFile = SerdesUtils.resourceToFile(file);
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);


        AvroToIon reader = AvroToIon.builder()
            .id(AvroToIonWriterTest.class.getSimpleName())
            .type(AvroToIon.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        AvroToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro writer = IonToAvro.builder()
            .id(IonToAvroTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .inferAllFields(Property.ofValue(false))
            .schema(
                Files.asCharSource(
                    new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader().getResource(file.replace(".avro", ".avsc"))).toURI()),
                    Charsets.UTF_8
                ).read()
            )
            .build();

        IonToAvro.Output run = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri())),
            is(IonToAvroTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                        .getResource(file))
                    .toURI())))
            )
        );
    }


}
