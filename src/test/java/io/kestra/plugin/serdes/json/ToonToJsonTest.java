package io.kestra.plugin.serdes.json;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ToonToJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    private ToonToJson.Output convert(File sourceFile) throws Exception {
        URI source = this.serdesUtils.resourceToStorageObject(sourceFile);

        ToonToJson task = ToonToJson.builder()
            .id(ToonToJson.class.getSimpleName())
            .type(ToonToJson.class.getName())
            .from(Property.ofValue(source.toString()))
            .build();

        return task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
    }

    private String readResult(URI uri) throws Exception {
        return IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, uri), StandardCharsets.UTF_8);
    }

    private JsonNode readJson(String content) throws Exception {
        return MAPPER.readTree(content);
    }

    @Test
    void simpleObject() throws Exception {
        String toon = """
            id: 123
            name: Ada
            active: true
            """;

        File temp = File.createTempFile("object_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "id": 123,
              "name": "Ada",
              "active": true
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
        assertThat(FilenameUtils.getExtension(output.getUri().getPath()), is("json"));
    }

    @Test
    void tabularArray() throws Exception {
        String toon = """
            users[2]{id,name,active}:
              1,Alice,true
              2,Bob,false
            """;

        File temp = File.createTempFile("tabular_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "users": [
                {"id": 1, "name": "Alice", "active": true},
                {"id": 2, "name": "Bob", "active": false}
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
        assertThat(output.getSize(), is(2L));
    }

    @Test
    void mixedArray() throws Exception {
        String toon = """
            items[3]:
              - 1
              - a: x
              - hello
            """;

        File temp = File.createTempFile("mixed_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "items": [
                1,
                {"a": "x"},
                "hello"
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void nestedObjects() throws Exception {
        String toon = """
            server:
              host: localhost
              port: 8080
              tags[2]: web,api
            """;

        File temp = File.createTempFile("nested_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "server": {
                "host": "localhost",
                "port": 8080,
                "tags": ["web", "api"]
              }
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void quotingAndSpecialChars() throws Exception {
        String toon = """
            urls[2]{id,url}:
              1,"http://a:b"
              2,"https://example.com?q=a:b"
            """;

        File temp = File.createTempFile("quote_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "urls": [
                {"id": 1, "url": "http://a:b"},
                {"id": 2, "url": "https://example.com?q=a:b"}
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void emptyArrayAndNull() throws Exception {
        String toon = """
            tags[0]:
            note: null
            """;

        File temp = File.createTempFile("empty_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "tags": [],
              "note": null
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void listItemTabularFirstField() throws Exception {
        String toon = """
            items[1]:
              - users[2]{id,name}:
                  1,Ada
                  2,Bob
                status: active
            """;

        File temp = File.createTempFile("list_tabular_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "items": [
                {
                  "users": [
                    {"id": 1, "name": "Ada"},
                    {"id": 2, "name": "Bob"}
                  ],
                  "status": "active"
                }
              ]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void unicodeAndEmoji() throws Exception {
        String toon = """
            message: Hello 世界 👋
            tags[3]: 🎉,🎊,🎈
            """;

        File temp = File.createTempFile("unicode_", ".toon");
        try (Writer w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        ToonToJson.Output output = convert(temp);
        String result = readResult(output.getUri());

        String expected = """
            {
              "message": "Hello 世界 👋",
              "tags": ["🎉", "🎊", "🎈"]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void quotedKeys() throws Exception {
        var toon = """
            "full name": Ada Lovelace
            "order:id": 7
            "say \\"hi\\"": hello
            "": empty
            "user info":
              "e-mail": ada@example.com
            "tags[1]"[2]: x,y
            rows[2]{"first name",age,"a,b"}:
              Ada,36,1
              Alan,41,2
            "list:items"[1]:
              - "item id": 1
                "item name": pen
            groups[1]:
              - "member list"[1]: Ada
            "a[0]{\\""[1]{b}:
              1
            """;

        var temp = File.createTempFile("quoted_keys_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        var output = convert(temp);
        var result = readResult(output.getUri());

        var expected = """
            {
              "full name": "Ada Lovelace",
              "order:id": 7,
              "say \\"hi\\"": "hello",
              "": "empty",
              "user info": {"e-mail": "ada@example.com"},
              "tags[1]": ["x", "y"],
              "rows": [
                {"first name": "Ada", "age": 36, "a,b": 1},
                {"first name": "Alan", "age": 41, "a,b": 2}
              ],
              "list:items": [{"item id": 1, "item name": "pen"}],
              "groups": [{"member list": ["Ada"]}],
              "a[0]{\\"": [{"b": 1}]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void quotedRootStringWithColon() throws Exception {
        var temp = File.createTempFile("quoted_root_string_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write("\"a:b\"");
        }

        var output = convert(temp);
        var result = readResult(output.getUri());

        assertThat(readJson(result), is(readJson("\"a:b\"")));
    }

    @Test
    void longTabularFieldNames() throws Exception {
        var name = "x".repeat(10_000);
        var temp = File.createTempFile("long_fields_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write("rows[1]{%1$s,\"%1$s y\"}:\n  1,2\n".formatted(name));
        }

        var output = convert(temp);
        var result = readResult(output.getUri());

        var expected = MAPPER.createObjectNode();
        expected.putArray("rows").addObject().put(name, 1).put("%s y".formatted(name), 2);
        assertThat(readJson(result), is(expected));
    }

    @Test
    void quotedKeysInRootTabularArray() throws Exception {
        var toon = """
            [2]{"first name","last:name","a}b","a\\",b"}:
              Ada,Lovelace,1,x
              Alan,Turing,2,y
            """;

        var temp = File.createTempFile("quoted_root_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        var output = convert(temp);
        var result = readResult(output.getUri());

        var expected = """
            [
              {"first name": "Ada", "last:name": "Lovelace", "a}b": 1, "a\\",b": "x"},
              {"first name": "Alan", "last:name": "Turing", "a}b": 2, "a\\",b": "y"}
            ]
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void escapedQuotesInValues() throws Exception {
        var toon = """
            rows[2]{name,note}:
              Ada,"a\\",b"
              Alan,plain
            tags[2]: "x\\",y",z
            """;

        var temp = File.createTempFile("escaped_values_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }

        var output = convert(temp);
        var result = readResult(output.getUri());

        var expected = """
            {
              "rows": [
                {"name": "Ada", "note": "a\\",b"},
                {"name": "Alan", "note": "plain"}
              ],
              "tags": ["x\\",y", "z"]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void roundTripQuotedKeysAndValues() throws Exception {
        var json = """
            {
              "full name": "Ada Lovelace",
              "order:id": 7,
              "say \\"hi\\"": "a\\",b",
              "": "empty",
              "user info": {"e-mail": "ada@example.com"},
              "tags[1]": ["x", "y,z", "q\\",r"],
              "rows": [
                {"first name": "Ada", "a,b": "x\\",y", "a}b": 1},
                {"first name": "Alan", "a,b": "z", "a}b": 2}
              ],
              "list:items": [{"item id": 1, "member list": ["Ada", "Alan"]}],
              "a[0]{\\"": [{"b": 1}]
            }
            """;

        var jsonFile = File.createTempFile("round_trip_", ".json");
        try (var w = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        var jsonToToon = JsonToToon.builder()
            .id(JsonToToon.class.getSimpleName())
            .type(JsonToToon.class.getName())
            .from(Property.ofValue(this.serdesUtils.resourceToStorageObject(jsonFile).toString()))
            .build();
        var toonOutput = jsonToToon.run(TestsUtils.mockRunContext(this.runContextFactory, jsonToToon, ImmutableMap.of()));

        var toonToJson = ToonToJson.builder()
            .id(ToonToJson.class.getSimpleName())
            .type(ToonToJson.class.getName())
            .from(Property.ofValue(toonOutput.getUri().toString()))
            .build();
        var jsonOutput = toonToJson.run(TestsUtils.mockRunContext(this.runContextFactory, toonToJson, ImmutableMap.of()));

        assertThat(readJson(readResult(jsonOutput.getUri())), is(readJson(json)));
    }

    private String convertToon(String toon) throws Exception {
        var temp = File.createTempFile("toon_", ".toon");
        try (var w = new FileWriter(temp, StandardCharsets.UTF_8)) {
            w.write(toon);
        }
        return readResult(convert(temp).getUri());
    }

    @Test
    void leadingZerosStayStrings() throws Exception {
        var result = convertToon("""
            zip: 05
            order_id: 007
            negative: -05
            codes[2]: 0123,-007
            rows[1]{code}:
              042
            """);

        var expected = """
            {
              "zip": "05",
              "order_id": "007",
              "negative": "-05",
              "codes": ["0123", "-007"],
              "rows": [{"code": "042"}]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void exponentsBecomeNumbers() throws Exception {
        var result = convertToon("""
            population: 1e6
            negative: -1E+03
            mantissa: 2.5e2
            ratio: 2.5e-1
            one: 5E+00
            zero: 0e1
            nums[2]: 1e3,3E-02
            """);

        var expected = """
            {
              "population": 1000000,
              "negative": -1000,
              "mantissa": 250,
              "ratio": 0.25,
              "one": 5,
              "zero": 0,
              "nums": [1000, 0.03]
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void exponentsOutsideLongRangeStayNumbers() throws Exception {
        var result = convertToon("""
            big: 1e30
            small: 1e-10
            huge: 1e999999999
            tooBig: 1e99999999999
            """);

        var json = MAPPER.reader().with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).readTree(result);

        assertThat(json.get("big").decimalValue().compareTo(new BigDecimal("1e30")), is(0));
        assertThat(json.get("small").decimalValue().compareTo(new BigDecimal("1e-10")), is(0));
        assertThat(json.get("huge").decimalValue().compareTo(new BigDecimal("1e999999999")), is(0));
        assertThat(json.get("tooBig").textValue(), is("1e99999999999"));
    }

    @Test
    void notJsonNumbersStayStrings() throws Exception {
        var result = convertToon("""
            a: .5
            b: 1.
            c: +5
            d: 1e
            e: 0x10
            f: 1_000
            """);

        var expected = """
            {"a": ".5", "b": "1.", "c": "+5", "d": "1e", "e": "0x10", "f": "1_000"}
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void plainNumbersKeepTheirValue() throws Exception {
        var result = convertToon("""
            zero: 0
            negativeZero: -0
            integer: 42
            decimal: 3.14
            beyondLong: 123456789012345678901234
            """);

        var expected = """
            {
              "zero": 0,
              "negativeZero": 0,
              "integer": 42,
              "decimal": 3.14,
              "beyondLong": 123456789012345678901234
            }
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void roundTripNumberLikeStrings() throws Exception {
        var json = """
            {"zip": "05", "exponent": "1e6", "negative": "-05", "number": 1000000, "small": 0.25}
            """;

        var jsonFile = File.createTempFile("round_trip_numbers_", ".json");
        try (var w = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
            w.write(json);
        }

        var jsonToToon = JsonToToon.builder()
            .id(JsonToToon.class.getSimpleName())
            .type(JsonToToon.class.getName())
            .from(Property.ofValue(this.serdesUtils.resourceToStorageObject(jsonFile).toString()))
            .build();
        var toonOutput = jsonToToon.run(TestsUtils.mockRunContext(this.runContextFactory, jsonToToon, ImmutableMap.of()));

        var toonToJson = ToonToJson.builder()
            .id(ToonToJson.class.getSimpleName())
            .type(ToonToJson.class.getName())
            .from(Property.ofValue(toonOutput.getUri().toString()))
            .build();
        var jsonOutput = toonToJson.run(TestsUtils.mockRunContext(this.runContextFactory, toonToJson, ImmutableMap.of()));

        assertThat(readJson(readResult(jsonOutput.getUri())), is(readJson(json)));
    }

    @Test
    void unicodeEscapes() throws Exception {
        var result = convertToon("""
            note: "a\\u00e9b"
            upper: "a\\u00ABb"
            control: "a\\u0004b"
            "k\\u00e9y": 1
            """);

        var expected = """
            {"note": "aéb", "upper": "a«b", "control": "a\\u0004b", "kéy": 1}
            """;

        assertThat(readJson(result), is(readJson(expected)));
    }

    @Test
    void invalidEscapesFail() {
        for (var toon : new String[] {
            "val: \"a\\qb\"",
            "val: \"a\\bb\"",
            "\"a\\x\": 1",
            "val: \"a\\u00b\"",
            "val: \"\\u+0ab\"",
            "val: \"\\u٠٠٤١\"",
            "val: \"a\\uD800b\"",
            "val: \"\\udc00\"",
            "val: \"\\ud83d\\ude00\"",
            "val: \"a\\\\\\\""
        }) {
            var exception = assertThrows(IllegalArgumentException.class, () -> convertToon(toon), toon);
            assertThat(exception.getMessage(), containsString("escape"));
        }
    }
}
