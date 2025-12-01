package io.kestra.plugin.serdes.yaml;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import static org.hamcrest.Matchers.is;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;

@KestraTest
class YamlToIonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storage;

    private URI put(String yaml) throws Exception {
        return storage.put(MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".yaml"),
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void yaml_docs_to_ion_values() throws Exception {
        URI src = put("""
            ---
            a: 1
            ---
            b: 2
            """);

        var task = YamlToIon.builder()
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));

        try (InputStream in = storage.get(MAIN_TENANT, null, out.getUri())) {
            IonSystem ion = IonSystemBuilder.standard().build();
            IonReader reader = ion.newReader(in);

            reader.next();
            IonValue v1 = ion.newValue(reader);
            assertThat(((IonInt) ((IonStruct) v1).get("a")).intValue(), is(1));

            reader.next();
            IonValue v2 = ion.newValue(reader);
            assertThat(((IonInt) ((IonStruct) v2).get("b")).intValue(), is(2));
        }
    }
}
