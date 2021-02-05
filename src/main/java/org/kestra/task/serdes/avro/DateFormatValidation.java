package org.kestra.task.serdes.avro;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import javax.validation.Constraint;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { })
public @interface DateFormatValidation {
    String message() default "invalid datetime value";
}
