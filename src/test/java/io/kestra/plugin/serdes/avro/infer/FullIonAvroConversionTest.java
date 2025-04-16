package io.kestra.plugin.serdes.avro.infer;

import com.amazon.ion.system.IonSystemBuilder;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.avro.AvroToIon;
import io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon;
import io.kestra.plugin.serdes.avro.IonToAvro;
import jakarta.inject.Inject;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
public class FullIonAvroConversionTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void allData_matching() throws Exception {
        var input = """
            {order_id:1,str1:"my string1",customer_name:"null",customer_email:"jenniferschneider@example.com",product_id:11,price:166.88999938964844e0,quantity:1,total:166.88999938964844e0}
            {order_id:2,str1:"my string2",customer_name:"null",customer_email:"22222222222222222@example.com",product_id:22,price:122.88999938964844e0,quantity:2,total:222.88999938964844e0}
            """;
        this.run(input, input);
    }

    @Test
    void someData_missingFields() throws Exception {
        this.run("""
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """,
            """
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {id:null, str1:"null", labels:null, data1: null}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """
        );// FIXME "null" String hack
    }

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

        // convert Ion to Avro using inferred schema
        var ionToAvro = IonToAvro.builder()
            .id(IonToAvro.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.of(inputIonFileUri.toString()))
            .schema(
                IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, inferAvroSchemaFromIonOutput.getUri())))
            )
            .build();
        var ionToAvroOutput = ionToAvro.run(TestsUtils.mockRunContext(runContextFactory, ionToAvro, ImmutableMap.of()));

        // convert back Avro into Ion
        var avroToIon = AvroToIon.builder()
            .id(AvroToIon.class.getSimpleName())
            .type(AvroToIon.class.getName())
            .from(Property.of(ionToAvroOutput.getUri().toString()))
            .build();
        var avroToIonOutput = avroToIon.run(TestsUtils.mockRunContext(runContextFactory, avroToIon, ImmutableMap.of()));

        // compare original Ion with generated after conversions
        var ion = IonSystemBuilder.standard().build();
        assertThat(
            IteratorUtils.toList(
                ion.iterate(IOUtils.toString(new InputStreamReader(storageInterface.get(null, null, avroToIonOutput.getUri()))))
            )
        ).isEqualTo(
            IteratorUtils.toList(
                ion.iterate(expectedOutputIon)
            )
        );
    }
}
