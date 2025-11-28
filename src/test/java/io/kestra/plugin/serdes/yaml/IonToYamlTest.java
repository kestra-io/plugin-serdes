package io.kestra.plugin.serdes.yaml;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
class IonToYamlTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storage;

    @Test
    void ion_multiple_to_yaml_docs() throws Exception {
        IonSystem ion = IonSystemBuilder.standard().build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter w = IonTextWriterBuilder.standard().build(out);

        IonStruct s1 = ion.newEmptyStruct();
        s1.put("a").newInt(1);
        s1.writeTo(w);

        IonStruct s2 = ion.newEmptyStruct();
        s2.put("b").newInt(2);
        s2.writeTo(w);

        w.close();

        URI src = storage.put(
            MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new ByteArrayInputStream(out.toByteArray())
        );

        var task = IonToYaml.builder()
            .from(Property.ofValue(src.toString()))
            .build();

        var result = task.run(runContextFactory.of(Map.of())).getUri();

        String yaml = new String(storage.get(MAIN_TENANT, null, result).readAllBytes(), StandardCharsets.UTF_8);

        System.out.println(yaml);
        assertThat(yaml, containsString("---\na: 1"));
        assertThat(yaml, containsString("---\nb: 2"));
    }
}
