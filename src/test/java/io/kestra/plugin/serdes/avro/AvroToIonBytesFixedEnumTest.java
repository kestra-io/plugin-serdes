package io.kestra.plugin.serdes.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Reproduces <a href="https://github.com/kestra-io/plugin-serdes/issues/373">#373</a>: an Avro file
 * written by {@link IonToAvro} with a {@code bytes}, {@code fixed} or {@code enum} field (and
 * therefore a {@code decimal}, which is a {@code bytes} logical type) could not be read back by
 * {@link AvroToIon}.
 */
@KestraTest
class AvroToIonBytesFixedEnumTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void bytes() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":"bytes"}]}""",
            List.of(Map.of("v", "abc"))
        );

        assertThat(rows, hasSize(1));
        assertThat(asMap(rows.getFirst()).get("v"), is("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void fixed() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":{"type":"fixed","name":"F","size":3}}]}""",
            List.of(Map.of("v", "abc"))
        );

        assertThat(rows, hasSize(1));
        assertThat(asMap(rows.getFirst()).get("v"), is("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void enums() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}""",
            List.of(Map.of("v", "B"))
        );

        assertThat(rows, hasSize(1));
        assertThat(asMap(rows.getFirst()).get("v"), is("B"));
    }

    @Test
    void decimal() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}""",
            List.of(Map.of("v", "12.34"))
        );

        assertThat(rows, hasSize(1));
        assertThat(asMap(rows.getFirst()).get("v"), is(new BigDecimal("12.34")));
    }

    @Test
    void nullableBytes() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":["null","bytes"],"default":null}]}""",
            List.of(Map.of("v", "abc"), java.util.Collections.singletonMap("v", null))
        );

        assertThat(rows, hasSize(2));
        assertThat(asMap(rows.getFirst()).get("v"), is("abc".getBytes(StandardCharsets.UTF_8)));
        assertThat(asMap(rows.get(1)).get("v"), is((Object) null));
    }

    @Test
    void bytesOfDecreasingLengthAreNotPaddedByRecordReuse() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":"bytes"}]}""",
            List.of(Map.of("v", "abcdef"), Map.of("v", "xy"))
        );

        assertThat(rows, hasSize(2));
        assertThat(asMap(rows.getFirst()).get("v"), is("abcdef".getBytes(StandardCharsets.UTF_8)));
        assertThat(asMap(rows.get(1)).get("v"), is("xy".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void arrayOfBytes() throws Exception {
        List<Object> rows = roundTrip(
            """
            {"type":"record","name":"T","fields":[{"name":"v","type":{"type":"array","items":"bytes"}}]}""",
            List.of(Map.of("v", List.of("ab", "cd")))
        );

        assertThat(rows, hasSize(1));
        assertThat(
            (List<?>) asMap(rows.getFirst()).get("v"),
            contains("ab".getBytes(StandardCharsets.UTF_8), "cd".getBytes(StandardCharsets.UTF_8))
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object row) {
        return (Map<String, Object>) row;
    }

    private List<Object> roundTrip(String schema, List<Map<String, Object>> ionRows) throws Exception {
        File ionFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(ionFile)) {
            for (Map<String, Object> row : ionRows) {
                FileSerde.write(output, row);
            }
        }

        URI source;
        try (var in = new FileInputStream(ionFile)) {
            source = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), in);
        }

        IonToAvro writer = IonToAvro.builder()
            .id(IdUtils.create())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(source.toString()))
            .inferAllFields(Property.ofValue(false))
            .schema(schema)
            .build();
        IonToAvro.Output avro = writer.run(TestsUtils.mockRunContext(runContextFactory, writer, ImmutableMap.of()));

        AvroToIon reader = AvroToIon.builder()
            .id(IdUtils.create())
            .type(AvroToIon.class.getName())
            .from(Property.ofValue(avro.getUri().toString()))
            .build();
        AvroToIon.Output ion = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, ion.getUri())) {
            return FileSerde.readAll(in).collectList().block();
        }
    }
}
