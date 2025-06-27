package io.kestra.plugin.serdes.avro;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ValidationFactoryTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void avroSchemaValidation() throws Exception {
        var schema = IOUtils.toString(
            Objects.requireNonNull(ValidationFactoryTest.class.getClassLoader().getResource("avro/s.avsc")),
            StandardCharsets.UTF_8
        );

        var validator = modelValidator.isValid(
            IonToAvro.builder()
                .id("unit")
                .from(Property.ofValue("unit"))
                .type(IonToAvro.class.getName())
                .schema(schema)
                .build()
        );
        assertThat(validator.isPresent(), is(false));

        validator = modelValidator.isValid(
            IonToAvro.builder()
                .id("unit")
                .from(Property.ofValue("unit"))
                .type(IonToAvro.class.getName())
                .schema("{\"invalid\": \"avro schema\"}")
                .build()
        );
        assertThat(validator.isPresent(), is(true));


        validator = modelValidator.isValid(
            IonToAvro.builder()
                .id("unit")
                .from(Property.ofValue("unit"))
                .type(IonToAvro.class.getName())
                .schema("{\"invalid json schema\"}")
                .build()
        );
        assertThat(validator.isPresent(), is(true));

        validator = modelValidator.isValid(
            IonToAvro.builder()
                .id("unit")
                .from(Property.ofValue("unit"))
                .type(IonToAvro.class.getName())
                .schema("{{ test }}")
                .build()
        );
        assertThat(validator.isPresent(), is(false));
    }
}
