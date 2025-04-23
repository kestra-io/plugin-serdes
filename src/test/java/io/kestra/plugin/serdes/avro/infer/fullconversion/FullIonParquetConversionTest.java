package io.kestra.plugin.serdes.avro.infer.fullconversion;

import com.amazon.ion.system.IonSystemBuilder;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon;
import io.kestra.plugin.serdes.parquet.IonToParquet;
import io.kestra.plugin.serdes.parquet.ParquetToIon;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class FullIonParquetConversionTest extends FullIonConversionAbstractTest {

    @Override
    void run(String ionInput, String expectedOutputIon) throws Exception {
        var ionInputStream = IOUtils.toInputStream(ionInput, StandardCharsets.UTF_8);
        var inputIonFileUri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), ionInputStream);

        // infer Avro schema from Ion
        var inferAvroSchemaFromIon = InferAvroSchemaFromIon.builder()
            .id(InferAvroSchemaFromIon.class.getSimpleName())
            .type(InferAvroSchemaFromIon.class.getName())
            .from(Property.of(inputIonFileUri.toString()))
            .build();
        var inferAvroSchemaFromIonOutput = inferAvroSchemaFromIon.run(TestsUtils.mockRunContext(runContextFactory, inferAvroSchemaFromIon, ImmutableMap.of()));

        var ionToParquet = IonToParquet.builder()
            .id(IonToParquet.class.getSimpleName())
            .type(IonToParquet.class.getName())
            .from(Property.of(inputIonFileUri.toString()))
            .schema(
                IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, inferAvroSchemaFromIonOutput.getUri())))
            )
            .build();
        var ionToParquetOutput = ionToParquet.run(TestsUtils.mockRunContext(runContextFactory, ionToParquet, ImmutableMap.of()));

        var parquetToIon = ParquetToIon.builder()
            .id(ParquetToIon.class.getSimpleName())
            .type(ParquetToIon.class.getName())
            .from(Property.of(ionToParquetOutput.getUri().toString()))
            .build();
        var parquetToIonOutput = parquetToIon.run(TestsUtils.mockRunContext(runContextFactory, parquetToIon, ImmutableMap.of()));

        // compare original Ion with generated after conversions
        var ion = IonSystemBuilder.standard().build();
        assertThat(
            IteratorUtils.toList(
                ion.iterate(IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, parquetToIonOutput.getUri()))))
            )
        ).isEqualTo(
            IteratorUtils.toList(
                ion.iterate(expectedOutputIon)
            )
        );
    }
}
