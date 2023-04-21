package io.kestra.plugin.serdes.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a XML file."
)
public class XmlWriter extends Task implements RunnableTask<XmlWriter.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    @PluginProperty(dynamic = true)
    private final String charset = StandardCharsets.UTF_8.name();

    @NotNull
    @Builder.Default
    @Schema(
        title = "Xml root name"
    )
    @PluginProperty(dynamic = true)
    private final String rootName = "items";

    @Builder.Default
    @Schema(
        title = "Timezone to use when no timezone can be parsed on the source."
    )
    @PluginProperty(dynamic = true)
    private final String timeZoneId = ZoneId.systemDefault().toString();

    @Override
    public XmlWriter.Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile(".xml").toFile();
        URI from = new URI(runContext.render(this.from));

        try (
            BufferedWriter outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)));
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
        ) {
            XmlMapper mapper = new XmlMapper();

            mapper
                .enable(SerializationFeature.INDENT_OUTPUT)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setSerializationInclusion(JsonInclude.Include.ALWAYS)
                .setTimeZone(TimeZone.getTimeZone(ZoneId.of(runContext.render(this.timeZoneId))))
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());


            ObjectWriter objectWriter = mapper.writer()
                .withRootName(runContext.render(this.rootName))
                .withFeatures(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);

            AtomicLong lineCount = new AtomicLong();

            List<Object> list = new ArrayList<>();
            FileSerde.reader(inputStream, throwConsumer(e -> {
                list.add(e);
                lineCount.incrementAndGet();
            }));
            outfile.write(objectWriter.writeValueAsString(list));
            runContext.metric(Counter.of("records", lineCount.get()));

            outfile.flush();
        }

        return XmlWriter.Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;
    }
}
