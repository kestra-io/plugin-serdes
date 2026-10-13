package io.kestra.plugin.serdes.json;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

final class SchemaRegistryClient {
    private static final Pattern POSITIVE_INTEGER = Pattern.compile("[1-9][0-9]{0,9}");
    private static final String ACCEPT = "application/vnd.schemaregistry.v1+json, application/json";

    record RegisteredSchema(String schema, Integer id, Integer version) {
    }

    private SchemaRegistryClient() {
    }

    static RegisteredSchema fetch(RunContext runContext, SchemaRegistrySource registry) throws Exception {
        var rUrl = runContext.render(registry.getUrl()).as(String.class)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("`registry.url` is required"));
        var rSubject = runContext.render(registry.getSubject()).as(String.class).orElse(null);
        var rId = runContext.render(registry.getId()).as(Integer.class).orElse(null);

        if ((rSubject == null || rSubject.isBlank()) == (rId == null)) {
            throw new IllegalArgumentException("Set exactly one of `registry.subject` or `registry.id`");
        }

        String path;
        String description;
        if (rId != null) {
            path = "/schemas/ids/" + rId;
            description = "schema id " + rId;
        } else {
            if (rSubject.equals(".") || rSubject.equals("..")) {
                throw new IllegalArgumentException("`registry.subject` cannot be `.` or `..`");
            }
            var rVersion = runContext.render(registry.getSubjectVersion()).as(String.class)
                .filter(value -> !value.isBlank())
                .orElse("latest");
            if (!rVersion.equals("latest") && !POSITIVE_INTEGER.matcher(rVersion).matches()) {
                throw new IllegalArgumentException("`registry.subjectVersion` must be a positive integer or `latest`, got '" + rVersion + "'");
            }
            path = "/subjects/" + encode(rSubject) + "/versions/" + encode(rVersion);
            description = "subject '" + rSubject + "' version '" + rVersion + "'";
        }

        runContext.logger().info("Fetching JSON Schema for {} from the schema registry", description);

        var request = HttpRequest.builder()
            .method("GET")
            .uri(URI.create(rUrl.replaceAll("/+$", "") + path))
            .addHeader("Accept", ACCEPT)
            .build();
        var options = registry.getOptions() != null ? registry.getOptions() : HttpConfiguration.builder().build();

        String body;
        try (var client = new HttpClient(runContext, options)) {
            body = client.request(request, String.class).getBody();
        } catch (HttpClientResponseException e) {
            throw translate(e, description);
        }

        if (body == null || body.isBlank()) {
            throw new IllegalStateException("The schema registry returned an empty response for " + description);
        }

        var json = JacksonMapper.ofJson().readTree(body);
        var schemaType = json.path("schemaType").asText("AVRO");
        if (!"JSON".equals(schemaType)) {
            throw new IllegalArgumentException(
                "The schema registry returned a schema of type " + schemaType + " for " + description
                    + ", but only JSON schemas are supported"
            );
        }
        var references = json.path("references");
        if (references.isArray() && !references.isEmpty()) {
            throw new IllegalArgumentException(
                "Schema references are not supported yet (" + description + " references "
                    + describeReferences(references) + ")"
            );
        }
        var schema = json.path("schema");
        if (!schema.isTextual() || schema.asText().isBlank()) {
            throw new IllegalStateException("The schema registry response for " + description + " has no `schema` field");
        }

        return new RegisteredSchema(
            schema.asText(),
            json.hasNonNull("id") ? json.get("id").asInt() : rId,
            json.hasNonNull("version") ? json.get("version").asInt() : null
        );
    }

    private static RuntimeException translate(HttpClientResponseException e, String description) {
        var status = e.getResponse().getStatus().getCode();
        return switch (status) {
            case 401, 403 -> new IllegalStateException(
                "The schema registry returned HTTP " + status + " for " + description
                    + ": check `registry.options.auth` and the permissions of the credentials",
                e
            );
            case 404 -> new IllegalStateException(
                "The schema registry has no schema for " + description + " (HTTP 404): check `registry.url`, the subject and the version", e
            );
            default -> new IllegalStateException(
                "The schema registry returned HTTP " + status + " for " + description, e
            );
        };
    }

    private static String describeReferences(JsonNode references) {
        var names = new StringBuilder();
        for (var reference : references) {
            if (!names.isEmpty()) {
                names.append(", ");
            }
            names.append(reference.path("subject").asText(reference.path("name").asText("?")));
        }
        return names.toString();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
