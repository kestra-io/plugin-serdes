package io.kestra.plugin.serdes.json;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JacksonException;
import com.networknt.schema.InputFormat;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Validate JSON data against a JSON Schema",
    description = """
        Validates a JSON payload against a JSON Schema, given inline or fetched from a Confluent-compatible \
        Schema Registry such as Karapace.

        The `$schema` keyword of the schema selects the draft; when it is absent, `defaultDialect` is used. \
        With draft-04 the validator mirrors [java-json-tools/json-schema-validator](https://github.com/java-json-tools/json-schema-validator): \
        the same set of checked `format` values (`date-time`, `email`, `hostname`, `ipv4`, `ipv6`, `regex`, `uri`, \
        `uuid`, `json-pointer`, `uri-template`, `base64`, `md5`, `sha1`, `sha256`, `sha512`, `mac`), ECMA-262 regular expressions, \
        and unknown keywords ignored. Keywords that need custom validator code are therefore not enforced.

        Remote `$ref` URLs are never fetched. The payload is read as a single JSON document; \
        JSONL files are not validated record by record."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Validate a payload against an inline JSON Schema and fail the task when it does not match.",
            code = """
                id: validate_json_inline
                namespace: company.team

                inputs:
                  - id: payload
                    type: JSON
                    defaults: '{"id": 1, "email": "ada@example.com"}'

                tasks:
                  - id: validate
                    type: io.kestra.plugin.serdes.json.ValidateJsonSchema
                    from: "{{ inputs.payload }}"
                    schema: |
                      {
                        "type": "object",
                        "required": ["id", "email"],
                        "properties": {
                          "id": {"type": "integer"},
                          "email": {"type": "string", "format": "email"}
                        }
                      }
                """
        ),
        @Example(
            full = true,
            title = "Validate a stored file against a namespace file schema, using draft-04 when the schema has no `$schema`.",
            code = """
                id: validate_json_namespace_file
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://example.com/data/orders.json

                  - id: validate
                    type: io.kestra.plugin.serdes.json.ValidateJsonSchema
                    from: "{{ outputs.download.uri }}"
                    schema: "{{ read('schemas/order.json') }}"
                    defaultDialect: DRAFT_4
                """
        ),
        @Example(
            full = true,
            title = "Validate against a schema stored in a Karapace registry and branch on the result instead of failing.",
            code = """
                id: validate_json_registry
                namespace: company.team

                tasks:
                  - id: fetch_order
                    type: io.kestra.plugin.core.http.Request
                    uri: https://example.com/api/orders/42

                  - id: validate
                    type: io.kestra.plugin.serdes.json.ValidateJsonSchema
                    from: "{{ outputs.fetch_order.body }}"
                    failOnInvalid: false
                    registry:
                      url: https://my-karapace.aivencloud.com:12345
                      subject: orders-value
                      version: latest
                      options:
                        auth:
                          type: BASIC
                          username: "{{ secret('REGISTRY_USERNAME') }}"
                          password: "{{ secret('REGISTRY_PASSWORD') }}"

                  - id: if_invalid
                    type: io.kestra.plugin.core.flow.If
                    condition: "{{ outputs.validate.valid == false }}"
                    then:
                      - id: report
                        type: io.kestra.plugin.core.log.Log
                        message: "Invalid order: {{ outputs.validate.errors }}"
                """
        )
    },
    metrics = {
        @Metric(name = "errors", description = "Number of validation errors found", type = Counter.TYPE)
    }
)
public class ValidateJsonSchema extends Task implements RunnableTask<ValidateJsonSchema.Output> {
    private static final String STORAGE_SCHEME = "kestra://";
    private static final int MAX_REPORTED_ERRORS = 100;
    private static final int MAX_ERRORS_IN_FAILURE_MESSAGE = 10;

    @NotNull
    @Schema(
        title = "JSON data to validate",
        description = """
            One of: a string containing JSON text, a `kestra://` internal storage URI of a JSON file (read as UTF-8), \
            or an object, array or scalar value, which is serialized to JSON before validation."""
    )
    @PluginProperty(group = "main")
    private Property<Object> from;

    @Schema(
        title = "Inline JSON Schema",
        description = """
            The JSON Schema as text. Use `{{ read('path/to/schema.json') }}` to load a namespace file. \
            Set exactly one of `schema` or `registry`."""
    )
    @PluginProperty(group = "main")
    private Property<String> schema;

    @Schema(
        title = "Schema Registry",
        description = "Fetch the schema from a Confluent-compatible Schema Registry. Set exactly one of `schema` or `registry`."
    )
    @PluginProperty(group = "connection")
    private SchemaRegistrySource registry;

    @Builder.Default
    @Schema(
        title = "Default dialect",
        description = """
            Draft used when the schema has no `$schema` keyword. One of `DRAFT_4`, `DRAFT_6`, `DRAFT_7`, \
            `DRAFT_2019_09` or `DRAFT_2020_12`. Defaults to `DRAFT_2020_12`, where `format` is annotation-only as the specification requires; \
            set `DRAFT_4` to match java-json-tools/json-schema-validator, which checks formats."""
    )
    @PluginProperty(group = "processing")
    private final Property<Dialect> defaultDialect = Property.ofValue(Dialect.DRAFT_2020_12);

    @Builder.Default
    @Schema(
        title = "Fail on invalid data",
        description = """
            When `true` (default), the task fails if the data does not match the schema. \
            When `false`, the task succeeds with `valid: false` and the list of `errors`."""
    )
    @PluginProperty(group = "processing")
    private final Property<Boolean> failOnInvalid = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rDialect = runContext.render(defaultDialect).as(Dialect.class).orElse(Dialect.DRAFT_2020_12);
        var rFailOnInvalid = runContext.render(failOnInvalid).as(Boolean.class).orElse(true);

        var rInlineSchema = schema == null ? null : runContext.render(schema).as(String.class).orElse(null);
        var hasInlineSchema = rInlineSchema != null && !rInlineSchema.isBlank();
        if (hasInlineSchema == (registry != null)) {
            throw new IllegalArgumentException("Set exactly one of `schema` or `registry`");
        }

        String schemaText;
        Integer schemaId = null;
        Integer schemaVersion = null;
        if (hasInlineSchema) {
            schemaText = rInlineSchema;
        } else {
            var registered = SchemaRegistryClient.fetch(runContext, registry);
            schemaText = registered.schema();
            schemaId = registered.id();
            schemaVersion = registered.version();
        }

        var payload = readPayload(runContext);

        com.networknt.schema.Schema jsonSchema;
        try {
            jsonSchema = JsonSchemaValidatorFactory.create(rDialect).getSchema(schemaText, InputFormat.JSON);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + e.getMessage(), e);
        }

        var errors = jsonSchema.validate(payload, InputFormat.JSON).stream()
            .map(
                error -> ValidationError.builder()
                    .instanceLocation(error.getInstanceLocation().toString())
                    .keyword(error.getKeyword())
                    .message(error.getMessage())
                    .schemaLocation(String.valueOf(error.getSchemaLocation()))
                    .build()
            )
            .toList();

        runContext.metric(Counter.of("errors", errors.size()));

        var reported = errors.size() > MAX_REPORTED_ERRORS ? errors.subList(0, MAX_REPORTED_ERRORS) : errors;
        if (!errors.isEmpty()) {
            reported.forEach(error -> runContext.logger().warn("{}: {}", error.getInstanceLocation(), error.getMessage()));
            if (reported.size() < errors.size()) {
                runContext.logger().warn("{} more validation errors not shown", errors.size() - reported.size());
            }
            if (rFailOnInvalid) {
                throw new IllegalStateException(failureMessage(errors));
            }
        }

        return Output.builder()
            .valid(errors.isEmpty())
            .errors(reported)
            .schemaId(schemaId)
            .schemaVersion(schemaVersion)
            .build();
    }

    private String readPayload(RunContext runContext) throws Exception {
        var rFrom = runContext.render(from).as(Object.class)
            .orElseThrow(() -> new IllegalArgumentException("`from` is required and cannot be empty"));

        String text;
        if (rFrom instanceof String value) {
            if (value.isBlank()) {
                throw new IllegalArgumentException("`from` is required and cannot be empty");
            }
            if (value.startsWith(STORAGE_SCHEME)) {
                try (var inputStream = runContext.storage().getFile(new URI(value))) {
                    text = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                text = value;
            }
        } else {
            text = JacksonMapper.ofJson().writeValueAsString(rFrom);
        }

        try {
            JacksonMapper.ofJson().readTree(text);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("`from` is not valid JSON: " + e.getOriginalMessage(), e);
        }
        return text;
    }

    private static String failureMessage(List<ValidationError> errors) {
        var message = new StringBuilder("JSON data does not match the schema (")
            .append(errors.size()).append(errors.size() == 1 ? " error" : " errors").append("):");
        errors.stream().limit(MAX_ERRORS_IN_FAILURE_MESSAGE).forEach(
            error -> message.append("\n- ").append(error.getInstanceLocation()).append(": ").append(error.getMessage())
        );
        if (errors.size() > MAX_ERRORS_IN_FAILURE_MESSAGE) {
            message.append("\n- ... and ").append(errors.size() - MAX_ERRORS_IN_FAILURE_MESSAGE).append(" more");
        }
        return message.toString();
    }

    public enum Dialect {
        DRAFT_4,
        DRAFT_6,
        DRAFT_7,
        DRAFT_2019_09,
        DRAFT_2020_12
    }

    @Getter
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ValidationError {
        @Schema(title = "JSON Pointer to the offending value in the data", description = "Empty for the document root.")
        private final String instanceLocation;

        @Schema(title = "Schema keyword that failed", description = "For example `required` or `type`.")
        private final String keyword;

        @Schema(title = "Human-readable error message")
        private final String message;

        @Schema(title = "Location of the failing keyword in the schema")
        private final String schemaLocation;
    }

    @Builder
    @Getter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Whether the data matches the schema")
        private final boolean valid;

        @Schema(
            title = "Validation errors",
            description = "Empty when valid. Limited to the first 100 errors; the `errors` metric holds the total."
        )
        private final List<ValidationError> errors;

        @Schema(title = "Registry schema ID", description = "Only set when the schema came from a registry.")
        private final Integer schemaId;

        @Schema(title = "Registry subject version", description = "Only set when the schema came from a registry subject.")
        private final Integer schemaVersion;
    }
}
