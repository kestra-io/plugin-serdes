package io.kestra.plugin.serdes.json;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.InputFormat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Runs the test vectors of java-json-tools/json-schema-validator (see NOTICE) against our draft-04 mode.
 */
class FgeParityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DRAFT_4 = "http://json-schema.org/draft-04/schema#";
    private static final String ROOT = "/jsonschema/fge/";

    private static boolean isValid(String schema, JsonNode data) throws Exception {
        var registry = JsonSchemaValidatorFactory.create(ValidateJsonSchema.Dialect.DRAFT_4);
        return registry.getSchema(schema, InputFormat.JSON)
            .validate(MAPPER.writeValueAsString(data), InputFormat.JSON)
            .isEmpty();
    }

    private static JsonNode load(String path) throws Exception {
        try (InputStream in = FgeParityTest.class.getResourceAsStream(ROOT + path)) {
            return MAPPER.readTree(in);
        }
    }

    private static Stream<DynamicTest> formatTests(String name, String directory) throws Exception {
        var schema = "{\"$schema\":\"" + DRAFT_4 + "\",\"format\":\"" + name + "\"}";
        var tests = new ArrayList<DynamicTest>();
        for (var vector : load("format/" + directory + "/" + name + ".json")) {
            var data = vector.get("data");
            if (name.equals("regex") && data.asText().contains("(?<=")) {
                continue; // Lookbehind is valid ECMAScript since ES2018; fge's Rhino/Nashorn engines reject it.
            }
            var expected = vector.get("valid").asBoolean();
            tests.add(dynamicTest(directory + "/" + name + ": " + data, () -> assertThat(isValid(schema, data), is(expected))));
        }
        return tests.stream();
    }

    @TestFactory
    Stream<DynamicTest> formats() throws Exception {
        var all = new ArrayList<DynamicTest>();
        for (var name : List.of("date-time", "email", "ipv6", "regex", "uri")) {
            formatTests(name, "common").forEach(all::add);
        }
        for (var name : List.of("hostname", "ipv4")) {
            formatTests(name, "draftv4").forEach(all::add);
        }
        for (var name : List.of("base64", "sha1", "uuid")) {
            formatTests(name, "extra").forEach(all::add);
        }
        return all.stream();
    }

    @TestFactory
    Stream<DynamicTest> patternKeyword() throws Exception {
        var tests = new ArrayList<DynamicTest>();
        for (var vector : load("keyword/pattern.json")) {
            var schema = MAPPER.writeValueAsString(vector.get("schema"));
            var data = vector.get("data");
            var expected = vector.get("valid").asBoolean();
            tests.add(dynamicTest("pattern: " + schema + " " + data, () -> assertThat(isValid(schema, data), is(expected))));
        }
        return tests.stream();
    }

    @TestFactory
    Stream<DynamicTest> draft4TestSuite() throws Exception {
        var tests = new ArrayList<DynamicTest>();
        for (var vector : load("testsuite-draftv4.json")) {
            var description = vector.get("description").asText();
            // fge falls back to draft-04 when $schema is absent, as does our default dialect here.
            var schema = MAPPER.writeValueAsString(vector.get("schema"));
            var data = vector.get("data");
            var expected = vector.get("valid").asBoolean();
            tests.add(dynamicTest(description, () -> assertThat(isValid(schema, data), is(expected))));
        }
        return tests.stream();
    }
}
