package io.kestra.plugin.serdes.avro;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = AvroSchemaValidator.class)
public @interface AvroSchemaValidation {
    String message() default "invalid avro schema";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
