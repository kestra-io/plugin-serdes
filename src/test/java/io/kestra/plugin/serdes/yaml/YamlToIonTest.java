package io.kestra.plugin.serdes.yaml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class YamlToIonTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storage;

    private URI put(String yaml) throws Exception {
        return storage.put(
            MAIN_TENANT, null,
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
        assertThat(out.getSize(), is(2L));
    }

    @Test
    void yaml_anchorsAliasesAndMergeKeys() throws Exception {
        URI src = put("""
            base: &b
              x: 1
            copy: *b
            merged:
              <<: *b
              y: 2
            """);

        var task = YamlToIon.builder()
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));

        try (InputStream in = storage.get(MAIN_TENANT, null, out.getUri())) {
            IonSystem ion = IonSystemBuilder.standard().build();
            IonReader reader = ion.newReader(in);

            reader.next();
            IonValue doc = ion.newValue(reader);
            IonStruct struct = (IonStruct) doc;

            assertThat(((IonInt) ((IonStruct) struct.get("base")).get("x")).intValue(), is(1));
            assertThat(((IonInt) ((IonStruct) struct.get("copy")).get("x")).intValue(), is(1));

            IonStruct merged = (IonStruct) struct.get("merged");
            assertThat(((IonInt) merged.get("x")).intValue(), is(1));
            assertThat(((IonInt) merged.get("y")).intValue(), is(2));
            assertThat(merged.get("<<"), is((IonValue) null));
        }
        assertThat(out.getSize(), is(1L));
    }

    @Test
    void yaml_dateLikeScalarsStayStrings() throws Exception {
        URI src = put("""
            date: 2024-01-01
            datetime: 2024-01-01T10:15:30Z
            """);

        var task = YamlToIon.builder()
            .from(Property.ofValue(src.toString()))
            .build();

        var out = task.run(runContextFactory.of(Map.of()));

        try (InputStream in = storage.get(MAIN_TENANT, null, out.getUri())) {
            IonSystem ion = IonSystemBuilder.standard().build();
            IonReader reader = ion.newReader(in);

            reader.next();
            IonStruct struct = (IonStruct) ion.newValue(reader);

            assertThat(((IonString) struct.get("date")).stringValue(), is("2024-01-01"));
            assertThat(((IonString) struct.get("datetime")).stringValue(), is("2024-01-01T10:15:30Z"));
        }
    }

    @Test
    void yaml_bombIsRejected() throws Exception {
        // Billion-laughs style alias amplification: each level references the previous one 10 times,
        // so aliases to non-scalar nodes pile up past SnakeYAML's default limit (50) well before any
        // expansion happens, instead of exhausting memory.
        URI src = put("""
            a: &a ["x","x","x","x","x","x","x","x","x","x"]
            b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]
            c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]
            d: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c,*c]
            e: &e [*d,*d,*d,*d,*d,*d,*d,*d,*d,*d]
            f: &f [*e,*e,*e,*e,*e,*e,*e,*e,*e,*e]
            g: [*f,*f,*f,*f,*f,*f,*f,*f,*f,*f]
            """);

        var task = YamlToIon.builder()
            .from(Property.ofValue(src.toString()))
            .build();

        var exception = assertThrows(Exception.class, () -> task.run(runContextFactory.of(Map.of())));
        assertThat(exception.getMessage(), containsString("aliases"));
    }
}
