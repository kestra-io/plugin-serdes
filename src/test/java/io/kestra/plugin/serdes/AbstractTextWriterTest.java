package io.kestra.plugin.serdes;

import java.time.ZonedDateTime;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.csv.IonToCsv;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class AbstractTextWriterTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void shouldKeepEachRunZoneWhenRunsOverlap() throws Exception {
        var writer = IonToCsv.builder()
            .id("csv")
            .type(IonToCsv.class.getName())
            .from(Property.ofValue("kestra:///unused.ion"))
            .timeZoneId(Property.ofExpression("{{ inputs.tz }}"))
            .build();
        var rcLisbon = TestsUtils.mockRunContext(runContextFactory, writer, Map.of("tz", "Europe/Lisbon"));
        var rcTokyo = TestsUtils.mockRunContext(runContextFactory, writer, Map.of("tz", "Asia/Tokyo"));
        var zdt = ZonedDateTime.parse("2021-05-05T12:21:12.123+02:00");

        var lisbon = writer.formatters(rcLisbon);
        var tokyo = writer.formatters(rcTokyo);

        assertThat(AbstractTextWriter.convert(zdt, lisbon), is("2021-05-05T11:21:12.123+01:00"));
        assertThat(AbstractTextWriter.convert(zdt, tokyo), is("2021-05-05T19:21:12.123+09:00"));
    }
}
