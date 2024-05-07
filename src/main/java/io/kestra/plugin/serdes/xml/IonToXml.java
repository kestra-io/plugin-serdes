package io.kestra.plugin.serdes.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a XML file."
)
@Plugin(
        examples = {
        @Example(
            full = true,
            title = "Read a CSV file, transform it and store the transformed data as an XML file.",
            code = """     
id: ion_to_xml
namespace: company.team

tasks:
  - id: download_csv
    type: io.kestra.plugin.core.http.Download
    description: salaries of data professionals from 2020 to 2023 (source ai-jobs.net)
    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/salaries.csv

  - id: avg_salary_by_job_title
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles:
      data.csv: "{{ outputs.download_csv.uri }}"
    sql: |
      SELECT 
        job_title,
        ROUND(AVG(salary),2) AS avg_salary
      FROM read_csv_auto('{{ workingDir }}/data.csv', header=True)
      GROUP BY job_title
      HAVING COUNT(job_title) > 10
      ORDER BY avg_salary DESC;
    store: true

  - id: result
    type: io.kestra.plugin.serdes.xml.IonToXml
    from: "{{ outputs.avg_salary_by_job_title.uri }}"
"""
        )
    },
    aliases = "io.kestra.plugin.serdes.xml.XmlWriter"
)
public class IonToXml extends Task implements RunnableTask<IonToXml.Output> {
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
    public IonToXml.Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".xml").toFile();
        URI from = new URI(runContext.render(this.from));

        try (
            Writer outfile = new BufferedWriter(new FileWriter(tempFile, Charset.forName(charset)), FileSerde.BUFFER_SIZE);
            Reader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from), Charset.forName(charset)), FileSerde.BUFFER_SIZE)
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

            List<Object> list = FileSerde.readAll(inputStream).collectList().block();
            if (list != null) {
                outfile.write(objectWriter.writeValueAsString(list));
                runContext.metric(Counter.of("records", list.size()));
            }

            outfile.flush();
        }

        return IonToXml.Output
            .builder()
            .uri(runContext.storage().putFile(tempFile))
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
