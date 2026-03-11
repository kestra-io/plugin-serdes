package io.kestra.plugin.serdes.avro;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = AvroSchemaValidator.class)
public @interface AvroSchemaValidation {
    String message() default "invalid avro schema";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
