package io.kestra.plugin.serdes.json;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import com.networknt.schema.dialect.DefaultDialectRegistry;
import com.networknt.schema.dialect.Dialect;
import com.networknt.schema.dialect.Draft4;
import com.networknt.schema.format.Format;
import com.networknt.schema.format.Formats;
import com.networknt.schema.path.PathType;
import com.networknt.schema.regex.JoniRegularExpressionFactory;

/**
 * Builds networknt registries that mimic java-json-tools/json-schema-validator ("fge") on draft-04:
 * same checked formats, ECMA-262 regexes, and unknown keywords ignored. Remote {@code $ref} stays off.
 */
final class JsonSchemaValidatorFactory {
    private static final Set<String> NETWORKNT_FORMATS = Set.of(
        "email", "hostname", "ipv4", "ipv6", "regex", "uri", "json-pointer", "uri-template"
    );

    private static final DefaultDialectRegistry DIALECTS = new DefaultDialectRegistry(List.of(fgeDraft4()));

    private static final SchemaRegistryConfig CONFIG = SchemaRegistryConfig.builder()
        .regularExpressionFactory(JoniRegularExpressionFactory.getInstance())
        .pathType(PathType.JSON_POINTER)
        .locale(Locale.ENGLISH)
        .build();

    private JsonSchemaValidatorFactory() {
    }

    static SchemaRegistry create(ValidateJsonSchema.Dialect defaultDialect) {
        var dialectId = SpecificationVersion.valueOf(defaultDialect.name()).getDialectId();
        return SchemaRegistry.withDefaultDialectId(
            dialectId,
            builder -> builder.dialectRegistry(DIALECTS).schemaRegistryConfig(CONFIG)
        );
    }

    private static Dialect fgeDraft4() {
        return Dialect.builder(Draft4.getInstance())
            .formats(map ->
            {
                map.clear();
                for (Format format : Formats.DEFAULT) {
                    if (NETWORKNT_FORMATS.contains(format.getName())) {
                        map.put(format.getName(), format);
                    }
                }
                for (Format format : FgeFormats.ALL) {
                    map.put(format.getName(), format);
                }
            })
            .build();
    }
}
