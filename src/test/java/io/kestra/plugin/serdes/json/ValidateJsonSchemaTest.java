package io.kestra.plugin.serdes.json;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.http.client.configurations.BasicAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ValidateJsonSchemaTest {
    private static final String ORDER_SCHEMA = """
        {
          "type": "object",
          "required": ["id", "email"],
          "properties": {
            "id": {"type": "integer"},
            "email": {"type": "string", "format": "email"}
          }
        }""";
    private static final String DRAFT_4 = "http://json-schema.org/draft-04/schema#";

    @Inject
    RunContextFactory runContextFactory;

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startRegistry() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stopRegistry() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        var path = exchange.getRequestURI().getRawPath();
        requests.add(path);
        authorizations.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));

        var status = 200;
        var body = switch (path) {
            case "/registry/subjects/orders-value/versions/latest" -> registered(ORDER_SCHEMA, 7, 3, "JSON", "[]");
            case "/registry/subjects/orders-value/versions/2" -> registered(ORDER_SCHEMA, 5, 2, "JSON", "[]");
            case "/registry/subjects/with%2Fslash/versions/latest" -> registered(ORDER_SCHEMA, 8, 1, "JSON", "[]");
            case "/registry/schemas/ids/7" -> registered(ORDER_SCHEMA, 7, null, "JSON", "[]");
            case "/registry/schemas/ids/9" -> registered("{\"type\":\"record\"}", 9, null, null, "[]");
            case "/registry/schemas/ids/10" -> registered(ORDER_SCHEMA, 10, null, "JSON", "[{\"name\":\"a\",\"subject\":\"common-value\",\"version\":1}]");
            case "/registry/subjects/secured/versions/latest" -> {
                if (
                    !("Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
                        .equals(exchange.getRequestHeaders().getFirst("Authorization"))
                ) {
                    status = 401;
                    yield "{\"error_code\":40101,\"message\":\"Unauthorized\"}";
                }
                yield registered(ORDER_SCHEMA, 11, 1, "JSON", "[]");
            }
            default -> {
                status = 404;
                yield "{\"error_code\":40401,\"message\":\"Subject not found\"}";
            }
        };

        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String registered(String schema, Integer id, Integer version, String schemaType, String references) {
        var json = new StringBuilder("{\"schema\":").append(quote(schema)).append(",\"id\":").append(id);
        if (version != null) {
            json.append(",\"version\":").append(version);
        }
        if (schemaType != null) {
            json.append(",\"schemaType\":\"").append(schemaType).append('"');
        }
        return json.append(",\"references\":").append(references).append('}').toString();
    }

    private static String quote(String value) {
        try {
            return JacksonMapper.ofJson().writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String registryUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/registry/";
    }

    private ValidateJsonSchema.ValidateJsonSchemaBuilder<?, ?> task() {
        return ValidateJsonSchema.builder().id(IdUtils.create()).type(ValidateJsonSchema.class.getName());
    }

    private ValidateJsonSchema.Output run(ValidateJsonSchema task) throws Exception {
        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    private ValidateJsonSchema.Output runInline(Object from, String schema, boolean failOnInvalid) throws Exception {
        return run(task().from(Property.ofValue(from)).schema(Property.ofValue(schema)).failOnInvalid(Property.ofValue(failOnInvalid)).build());
    }

    @Test
    void validStringPayload() throws Exception {
        var output = runInline("{\"id\": 1, \"email\": \"ada@example.com\"}", ORDER_SCHEMA, true);

        assertThat(output.isValid(), is(true));
        assertThat(output.getErrors(), is(empty()));
        assertThat(output.getSchemaId(), is(nullValue()));
    }

    @Test
    void validMapPayload() throws Exception {
        var output = runInline(Map.of("id", 1, "email", "ada@example.com"), ORDER_SCHEMA, true);

        assertThat(output.isValid(), is(true));
    }

    @Test
    void validListAndScalarRoots() throws Exception {
        assertThat(runInline(List.of(1, 2, 3), "{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}", true).isValid(), is(true));
        assertThat(runInline(42, "{\"type\":\"integer\"}", true).isValid(), is(true));
        assertThat(runInline("\"hello\"", "{\"type\":\"integer\"}", false).isValid(), is(false));
    }

    @Test
    void storageUriPayload() throws Exception {
        var file = File.createTempFile("payload_", ".json");
        Files.writeString(file.toPath(), "{\"id\": \"not-an-int\", \"email\": \"ada@example.com\"}");
        var runContext = runContextFactory.of();
        var uri = runContext.storage().putFile(file);

        var task = task().from(Property.ofValue(uri.toString())).schema(Property.ofValue(ORDER_SCHEMA))
            .failOnInvalid(Property.ofValue(false)).build();
        var output = task.run(runContext);

        assertThat(output.isValid(), is(false));
        assertThat(output.getErrors().getFirst().getInstanceLocation(), is("/id"));
        assertThat(output.getErrors().getFirst().getKeyword(), is("type"));
    }

    @Test
    void invalidPayloadReportsStructuredErrors() throws Exception {
        var schema = "{\"$schema\": \"" + DRAFT_4 + "\"," + ORDER_SCHEMA.substring(1);
        var output = runInline("{\"email\": \"nope\"}", schema, false);

        assertThat(output.isValid(), is(false));
        assertThat(output.getErrors().stream().map(ValidateJsonSchema.ValidationError::getKeyword).sorted().toList(), contains("format", "required"));
        var required = output.getErrors().stream().filter(error -> error.getKeyword().equals("required")).findFirst().orElseThrow();
        assertThat(required.getInstanceLocation(), is(""));
        assertThat(required.getMessage(), containsString("id"));
        var format = output.getErrors().stream().filter(error -> error.getKeyword().equals("format")).findFirst().orElseThrow();
        assertThat(format.getInstanceLocation(), is("/email"));
        assertThat(format.getSchemaLocation(), notNullValue());
    }

    @Test
    void failOnInvalidThrowsReadableMessage() {
        var exception = assertThrows(IllegalStateException.class, () -> runInline("{\"id\": \"x\", \"email\": \"ada@example.com\"}", ORDER_SCHEMA, true));

        assertThat(exception.getMessage(), containsString("does not match the schema (1 error)"));
        assertThat(exception.getMessage(), containsString("/id"));
    }

    @Test
    void failureMessageIsCappedButMetricsKeepTheTotal() throws Exception {
        var items = new StringBuilder("[");
        for (var i = 0; i < 150; i++) {
            items.append(i == 0 ? "" : ",").append("\"x\"");
        }
        items.append("]");
        var schema = "{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}";

        var exception = assertThrows(IllegalStateException.class, () -> runInline(items.toString(), schema, true));
        assertThat(exception.getMessage(), containsString("(150 errors)"));
        assertThat(exception.getMessage(), containsString("... and 140 more"));

        var output = runInline(items.toString(), schema, false);
        assertThat(output.getErrors().size(), is(100));
    }

    @Test
    void malformedPayloadFailsClearly() {
        var exception = assertThrows(IllegalArgumentException.class, () -> runInline("{\"id\": ", ORDER_SCHEMA, true));

        assertThat(exception.getMessage(), containsString("`from` is not valid JSON"));
    }

    @Test
    void malformedSchemaFailsClearly() {
        var exception = assertThrows(IllegalArgumentException.class, () -> runInline("{}", "{\"type\": ", true));

        assertThat(exception.getMessage(), containsString("Invalid JSON Schema"));
    }

    @Test
    void blankFromFailsClearly() {
        var exception = assertThrows(IllegalArgumentException.class, () -> runInline("  ", ORDER_SCHEMA, true));

        assertThat(exception.getMessage(), containsString("`from` is required"));
    }

    @Test
    void exactlyOneSchemaSourceIsRequired() {
        var none = task().from(Property.ofValue("{}")).build();
        assertThat(assertThrows(IllegalArgumentException.class, () -> run(none)).getMessage(), containsString("exactly one of `schema` or `registry`"));

        var both = task().from(Property.ofValue("{}")).schema(Property.ofValue("{}"))
            .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).id(Property.ofValue(7)).build()).build();
        assertThat(assertThrows(IllegalArgumentException.class, () -> run(both)).getMessage(), containsString("exactly one of `schema` or `registry`"));
    }

    @Test
    void schemaDialectSelection() throws Exception {
        var booleanExclusive = "{\"maximum\": 10, \"exclusiveMaximum\": true}";

        var explicitDraft4 = "{\"$schema\": \"" + DRAFT_4 + "\", \"maximum\": 10, \"exclusiveMaximum\": true}";
        assertThat(runInline(10, explicitDraft4, false).isValid(), is(false));
        assertThat(runInline(9, explicitDraft4, false).isValid(), is(true));

        var implicitDraft4 = task().from(Property.ofValue(10)).schema(Property.ofValue(booleanExclusive))
            .defaultDialect(Property.ofValue(ValidateJsonSchema.Dialect.DRAFT_4)).failOnInvalid(Property.ofValue(false)).build();
        assertThat(run(implicitDraft4).isValid(), is(false));

        var draft7Override = "{\"$schema\": \"http://json-schema.org/draft-07/schema#\", \"exclusiveMaximum\": 10}";
        var overridden = task().from(Property.ofValue(10)).schema(Property.ofValue(draft7Override))
            .defaultDialect(Property.ofValue(ValidateJsonSchema.Dialect.DRAFT_4)).failOnInvalid(Property.ofValue(false)).build();
        assertThat(run(overridden).isValid(), is(false));
    }

    @Test
    void draft4FormatSetMirrorsFge() throws Exception {
        Map<String, String> formatToInvalidValue = Map.of(
            "md5", "xyz",
            "sha1", "abc",
            "sha256", "abc",
            "sha512", "abc",
            "base64", "abc",
            "mac", "00:11:22:33:44",
            "uuid", "not-a-uuid",
            "ipv4", "999.1.1.1"
        );
        for (var entry : formatToInvalidValue.entrySet()) {
            var schema = "{\"$schema\": \"" + DRAFT_4 + "\", \"format\": \"" + entry.getKey() + "\"}";
            assertThat(entry.getKey(), runInline("\"" + entry.getValue() + "\"", schema, false).isValid(), is(false));
        }
        assertThat(runInline("\"d41d8cd98f00b204e9800998ecf8427e\"", "{\"$schema\": \"" + DRAFT_4 + "\", \"format\": \"md5\"}", true).isValid(), is(true));
        assertThat(runInline("\"00:1A:2b:3C:4d:5E\"", "{\"$schema\": \"" + DRAFT_4 + "\", \"format\": \"mac\"}", true).isValid(), is(true));

        for (var unchecked : List.of("date", "time", "duration", "uri-reference", "iri", "idn-email", "phone", "color", "alpha")) {
            var schema = "{\"$schema\": \"" + DRAFT_4 + "\", \"format\": \"" + unchecked + "\"}";
            assertThat(unchecked, runInline("\"%%% not valid %%%\"", schema, false).isValid(), is(true));
        }
    }

    @Test
    void patternUsesEcmaSemantics() throws Exception {
        var schema = "{\"$schema\": \"" + DRAFT_4 + "\", \"pattern\": \"^[^]$\"}";

        assertThat(runInline("\"a\"", schema, false).isValid(), is(true));
        assertThat(runInline("\"ab\"", schema, false).isValid(), is(false));
        assertThat(runInline("\"٣\"", "{\"pattern\": \"^\\\\d$\"}", false).isValid(), is(false));
    }

    @Test
    void unknownKeywordsAreIgnored() throws Exception {
        var schema = "{\"type\": \"object\", \"x-company-rule\": {\"must\": \"never be enforced\"}}";

        assertThat(runInline("{\"a\": 1}", schema, true).isValid(), is(true));
    }

    @Test
    void remoteRefIsNotFetched() {
        var schema = "{\"$ref\": \"http://127.0.0.1:" + server.getAddress().getPort() + "/remote.json\"}";

        assertThrows(RuntimeException.class, () -> runInline("{}", schema, true));
        assertThat(requests, is(empty()));
    }

    @Test
    void registryBySubjectLatest() throws Exception {
        var output = run(
            task().from(Property.ofValue("{\"id\": 1, \"email\": \"ada@example.com\"}"))
                .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("orders-value")).build())
                .build()
        );

        assertThat(output.isValid(), is(true));
        assertThat(output.getSchemaId(), is(7));
        assertThat(output.getSchemaVersion(), is(3));
        assertThat(requests, contains("/registry/subjects/orders-value/versions/latest"));
    }

    @Test
    void registryBySubjectVersionWithEncodedSubject() throws Exception {
        var byVersion = run(
            task().from(Property.ofValue("{\"id\": 1, \"email\": \"ada@example.com\"}"))
                .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("orders-value")).version(Property.ofValue("2")).build())
                .build()
        );
        assertThat(byVersion.getSchemaVersion(), is(2));

        var slash = run(
            task().from(Property.ofValue("{\"id\": 1, \"email\": \"ada@example.com\"}"))
                .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("with/slash")).build())
                .build()
        );
        assertThat(slash.getSchemaId(), is(8));
    }

    @Test
    void registryById() throws Exception {
        var output = run(
            task().from(Property.ofValue("{\"id\": \"x\", \"email\": \"ada@example.com\"}")).failOnInvalid(Property.ofValue(false))
                .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).id(Property.ofValue(7)).build())
                .build()
        );

        assertThat(output.isValid(), is(false));
        assertThat(output.getSchemaId(), is(7));
        assertThat(output.getSchemaVersion(), is(nullValue()));
        assertThat(requests, contains("/registry/schemas/ids/7"));
    }

    @Test
    void registrySendsBasicAuth() throws Exception {
        var options = HttpConfiguration.builder()
            .auth(BasicAuthConfiguration.builder().username(Property.ofValue("user")).password(Property.ofValue("pass")).build())
            .build();

        var output = run(
            task().from(Property.ofValue("{\"id\": 1, \"email\": \"ada@example.com\"}"))
                .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("secured")).options(options).build())
                .build()
        );

        assertThat(output.isValid(), is(true));
        assertThat(authorizations.getFirst(), is("Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void registryRejectsMissingCredentials() {
        var exception = assertThrows(
            IllegalStateException.class, () -> run(
                task().from(Property.ofValue("{}"))
                    .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("secured")).build())
                    .build()
            )
        );

        assertThat(exception.getMessage(), containsString("HTTP 401"));
        assertThat(exception.getMessage(), containsString("registry.options.auth"));
    }

    @Test
    void registryUnknownSubject() {
        var exception = assertThrows(
            IllegalStateException.class, () -> run(
                task().from(Property.ofValue("{}"))
                    .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).subject(Property.ofValue("missing")).build())
                    .build()
            )
        );

        assertThat(exception.getMessage(), containsString("subject 'missing' version 'latest'"));
        assertThat(exception.getMessage(), containsString("HTTP 404"));
    }

    @Test
    void registryRejectsNonJsonSchemaType() {
        var exception = assertThrows(
            IllegalArgumentException.class, () -> run(
                task().from(Property.ofValue("{}"))
                    .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).id(Property.ofValue(9)).build())
                    .build()
            )
        );

        assertThat(exception.getMessage(), containsString("type AVRO"));
        assertThat(exception.getMessage(), containsString("schema id 9"));
    }

    @Test
    void registryRejectsReferences() {
        var exception = assertThrows(
            IllegalArgumentException.class, () -> run(
                task().from(Property.ofValue("{}"))
                    .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).id(Property.ofValue(10)).build())
                    .build()
            )
        );

        assertThat(exception.getMessage(), containsString("Schema references are not supported yet"));
        assertThat(exception.getMessage(), containsString("common-value"));
    }

    @Test
    void registryRequiresSubjectOrId() {
        var exception = assertThrows(
            IllegalArgumentException.class, () -> run(
                task().from(Property.ofValue("{}"))
                    .registry(SchemaRegistrySource.builder().url(Property.ofValue(registryUrl())).build())
                    .build()
            )
        );

        assertThat(exception.getMessage(), containsString("exactly one of `registry.subject` or `registry.id`"));
    }

    @Test
    void taskIsReadFromYaml() throws Exception {
        var yaml = """
            id: validate
            type: io.kestra.plugin.serdes.json.ValidateJsonSchema
            from: '{"id": 1, "email": "ada@example.com"}'
            defaultDialect: DRAFT_4
            registry:
              url: "%s"
              subject: secured
              options:
                auth:
                  type: BASIC
                  username: user
                  password: pass
            """.formatted(registryUrl());

        var task = JacksonMapper.ofYaml().readValue(yaml, ValidateJsonSchema.class);

        assertThat(run(task).getSchemaId(), is(11));
    }
}
