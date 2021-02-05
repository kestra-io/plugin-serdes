package org.kestra.task.serdes.avro;

import io.micronaut.context.annotation.Factory;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.text.SimpleDateFormat;
import java.util.Date;
import javax.inject.Singleton;

@Factory
public class ValidatorFactory {
    @Singleton
    ConstraintValidator<DateFormatValidation, String> dateTimeValidator() {
        return (value, annotationMetadata, context) -> {
            if (value == null) {
                return true; // nulls are allowed according to spec
            }

            try {
                Date now = new Date();
                java.text.DateFormat d = new SimpleDateFormat(value);
                d.format(now);
            } catch (Exception e) {
                return false;
            }
            return true;
        };
    }

    @Singleton
    ConstraintValidator<AvroSchemaValidation, String> validAvroSchemaValidator() {
        return (value, annotationMetadata, context) -> {
            if (value == null) {
                return true; // nulls are allowed according to spec
            }

            try {
                final Schema.Parser parser = new Schema.Parser();
                parser.parse(value);
            } catch (SchemaParseException e) {
                return false;
            }
            return true;
        };
    }
}
