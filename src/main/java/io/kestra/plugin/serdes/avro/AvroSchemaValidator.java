package io.kestra.plugin.serdes.avro;

import io.kestra.core.validations.CronExpression;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import io.micronaut.validation.validator.constraints.ConstraintValidatorContext;
import jakarta.inject.Singleton;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

@Singleton
@Introspected
public class AvroSchemaValidator implements ConstraintValidator<AvroSchemaValidation, String> {
    @Override
    public boolean isValid(
        @Nullable String value,
        @NonNull AnnotationValue<AvroSchemaValidation> annotationMetadata,
        @NonNull ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        // pebble variable so can't validate
        if (value.contains("{{") && value.contains("}}")) {
            return true;
        }

        try {
            final Schema.Parser parser = new Schema.Parser();
            parser.parse(value);
        } catch (SchemaParseException e) {
            context.messageTemplate("invalid avro schema '({validatedValue})': " + e.getMessage());

            return false;
        }
        return true;
    }
}