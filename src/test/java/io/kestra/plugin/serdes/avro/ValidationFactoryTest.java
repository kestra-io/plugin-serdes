package io.kestra.plugin.serdes.avro;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.validations.ModelValidator;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import javax.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ValidationFactoryTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void dateFormatValidation() throws Exception {
        assertThat(getOptionsDate("YYYY","YYYYHH:mm","H:mm").isPresent(), is(false));
        assertThat(getOptionsDate("YYYYo","YYYYHH:mm","HH:mm").isPresent(), is(true));
        assertThat(getOptionsDate("YYYYo","YYYYHH:mm","YYYY").get().getConstraintViolations().size(), is(1));
        assertThat(getOptionsDate("YYYYo","YYYYHH:mmo","H:mm").get().getConstraintViolations().size(), is(2));
        assertThat(getOptionsDate("YYYYo","YYYYHH:mmo","H:mmo").get().getConstraintViolations().size(), is(3));
    }

    private Optional<ConstraintViolationException> getOptionsDate(String d1, String d2, String d3)  throws  Exception{
        var schema = IOUtils.toString(
            Objects.requireNonNull(ValidationFactoryTest.class.getClassLoader().getResource("avro/s.avsc")),
            StandardCharsets.UTF_8
        );

        var options =  AvroWriter.builder()
            .id("unit")
            .from("unit")
            .type(AvroWriter.class.getName())
            .schema(schema)
            .dateFormat(d1)
            .datetimeFormat(d2)
            .timeFormat(d3)
            .build();

        return modelValidator.isValid(options);
    }


    @Test
    void avroSchemaValidation() throws Exception {
        var schema = IOUtils.toString(
            Objects.requireNonNull(ValidationFactoryTest.class.getClassLoader().getResource("avro/s.avsc")),
            StandardCharsets.UTF_8
        );

        var validator = modelValidator.isValid(
            AvroWriter.builder()
                .id("unit")
                .from("unit")
                .type(AvroWriter.class.getName())
                .schema(schema)
                .build()
        );
        assertThat(validator.isPresent(), is(false));

        validator = modelValidator.isValid(
            AvroWriter.builder()
                .id("unit")
                .from("unit")
                .type(AvroWriter.class.getName())
                .schema("{\"invalid\": \"avro schema\"}")
                .build()
        );
        assertThat(validator.isPresent(), is(true));
    }
}
