package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategies;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.serdes.AbstractTextWriter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read an ion serialized data file and write it to a csv file."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Download a CSV file, transform it in SQL and store the transformed data as a CSV file.",
            code = """     
id: ion_to_csv
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
    type: io.kestra.plugin.serdes.csv.IonToCsv
    from: "{{ outputs.avg_salary_by_job_title.uri }}"
"""
        )
    },
    aliases = "io.kestra.plugin.serdes.csv.CsvWriter"
)
public class IonToCsv extends AbstractTextWriter implements RunnableTask<IonToCsv.Output> {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header"
    )
    @PluginProperty
    private final Boolean header = true;

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    @PluginProperty
    private final Character fieldSeparator = ',';

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    @PluginProperty
    private final Character textDelimiter = '"';

    @Builder.Default
    @Schema(
        title = "The character used to separate rows"
    )
    @PluginProperty
    private final String lineDelimiter = "\n";

    @Builder.Default
    @Schema(
        title = "Whether fields should always be delimited using the textDelimiter option."
    )
    @PluginProperty
    private final Boolean alwaysDelimitText = false;

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    @PluginProperty
    private final String charset = StandardCharsets.UTF_8.name();


    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.workingDir().createTempFile(".csv").toFile();

        // reader
        URI from = new URI(runContext.render(this.from));

        // formatter
        this.init(runContext);

        try (
            Reader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            Writer fileWriter = new BufferedWriter(new  FileWriter(tempFile, Charset.forName(this.charset)), FileSerde.BUFFER_SIZE);
            de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter(fileWriter)
        ) {

            
            Flux<Object> flowable = FileSerde.readAll(inputStream)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void accept(Object row) {
                        if (row instanceof List) {
                            List<Object> casted = (List<Object>) row;

                            if (header) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            var record = casted.stream().map(field -> convert(field)).toList();
                            csvWriter.writeRecord(record);
                        } else if (row instanceof Map) {
                            Map<String, Object> casted = (Map<String, Object>) row;

                            if (!first) {
                                this.first = true;
                                if (header) {
                                    var record = casted.keySet().stream().map(field -> convert(field)).toList();
                                    csvWriter.writeRecord(record);
                                }
                            }

                            var record = casted.values().stream().map(field -> convert(field)).toList();
                            csvWriter.writeRecord(record);
                        }
                    }
                });

            // metrics & finalize
            Mono<Long> count = flowable.count();
            Long lineCount = count.block();
            runContext.metric(Counter.of("records", lineCount));
        }

        return Output
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
        private URI uri;
    }

    private de.siegmar.fastcsv.writer.CsvWriter csvWriter(Writer writer) {
        var builder = de.siegmar.fastcsv.writer.CsvWriter.builder();

        builder.quoteCharacter(this.textDelimiter);
        builder.fieldSeparator(this.fieldSeparator);
        builder.lineDelimiter(LineDelimiter.of(this.lineDelimiter));
        if (this.alwaysDelimitText) {
            builder.quoteStrategy(QuoteStrategies.ALWAYS);
        }

        return builder.build(writer);
    }
}
