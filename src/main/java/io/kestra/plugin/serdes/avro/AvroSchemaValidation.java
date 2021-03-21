package io.kestra.plugin.serdes.avro;

import javax.validation.Constraint;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { })
public @interface AvroSchemaValidation {
    String message() default "invalid avro schema";
}
