package io.kestra.plugin.serdes.avro;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

public class AvroSchemaValidator implements ConstraintValidator<AvroSchemaValidation, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
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
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("invalid avro schema '({validatedValue})': " + e.getMessage())
                .addConstraintViolation();

            return false;
        }
        return true;
    }
}