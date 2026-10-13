package io.kestra.plugin.serdes.json;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@Schema(
    title = "Confluent-compatible Schema Registry",
    description = """
        Fetches a JSON Schema from a Confluent-compatible Schema Registry such as Karapace. \
        Set either `subject` (with an optional `subjectVersion`) or `id`. Only registered schemas of type \
        `JSON` without `references` are supported."""
)
public class SchemaRegistrySource {
    @Schema(
        title = "Registry URL",
        description = "Base URL of the registry, for example `https://my-registry.example.com:12345`. A path prefix is allowed."
    )
    @PluginProperty(group = "connection")
    private Property<String> url;

    @Schema(
        title = "Subject",
        description = "Subject holding the schema. Required unless `id` is set."
    )
    @PluginProperty(group = "main")
    private Property<String> subject;

    @Schema(
        title = "Subject version",
        description = "Version of the subject: an integer or `latest`, which is the default. Ignored when `id` is set."
    )
    @PluginProperty(group = "main")
    private Property<String> subjectVersion;

    @Schema(
        title = "Global schema ID",
        description = "Registry-wide schema ID. Mutually exclusive with `subject`."
    )
    @PluginProperty(group = "main")
    private Property<Integer> id;

    @Schema(
        title = "HTTP options",
        description = "Authentication (basic or bearer), SSL, proxy and timeout settings used to call the registry."
    )
    @PluginProperty(group = "connection")
    private HttpConfiguration options;
}
