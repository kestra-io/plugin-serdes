package io.kestra.plugin.serdes.avro.infer.fullconversion;

import com.amazon.ion.system.IonSystemBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.avro.AvroToIon;
import io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon;
import io.kestra.plugin.serdes.avro.IonToAvro;
import org.apache.commons.io.IOUtils;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class FullIonAvroConversionTest extends FullIonConversionAbstractTest {

    @Override
    void run(String ionInput, String expectedOutputIon) throws Exception {
        var ionInputStream = IOUtils.toInputStream(ionInput, StandardCharsets.UTF_8);
        var inputIonFileUri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), ionInputStream);

        // infer Avro schema from Ion
        var inferAvroSchemaFromIon = InferAvroSchemaFromIon.builder()
            .id(InferAvroSchemaFromIon.class.getSimpleName())
            .type(InferAvroSchemaFromIon.class.getName())
            .from(Property.ofValue(inputIonFileUri.toString()))
            .build();
        var inferAvroSchemaFromIonOutput = inferAvroSchemaFromIon.run(TestsUtils.mockRunContext(runContextFactory, inferAvroSchemaFromIon, ImmutableMap.of()));

        // convert Ion to Avro using inferred schema
        var ionToAvro = IonToAvro.builder()
            .id(IonToAvro.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(inputIonFileUri.toString()))
            .schema(
                IOUtils.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, inferAvroSchemaFromIonOutput.getUri())))
            )
            .build();
        var ionToAvroOutput = ionToAvro.run(TestsUtils.mockRunContext(runContextFactory, ionToAvro, ImmutableMap.of()));

        // convert back Avro into Ion
        var avroToIon = AvroToIon.builder()
            .id(AvroToIon.class.getSimpleName())
            .type(AvroToIon.class.getName())
            .from(Property.ofValue(ionToAvroOutput.getUri().toString()))
            .build();
        var avroToIonOutput = avroToIon.run(TestsUtils.mockRunContext(runContextFactory, avroToIon, ImmutableMap.of()));

        // compare original Ion with generated after conversions
        var ion = IonSystemBuilder.standard().build();
        assertThat(
            ImmutableList.copyOf(
                ion.iterate(IOUtils.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, avroToIonOutput.getUri()))))
            )
        ).isEqualTo(
            ImmutableList.copyOf(
                ion.iterate(expectedOutputIon)
            )
        );
    }
}
