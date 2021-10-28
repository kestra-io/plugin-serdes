package io.kestra.plugin.serdes.avro;

import io.micronaut.context.annotation.Factory;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import javax.inject.Singleton;

@Factory
public class ValidatorFactory {
    @Singleton
    ConstraintValidator<AvroSchemaValidation, String> validAvroSchemaValidator() {
        return (value, annotationMetadata, context) -> {
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
        };
    }
}
