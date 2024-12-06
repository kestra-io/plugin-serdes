package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategies;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.serdes.AbstractTextWriter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header"
    )
    private final Property<Boolean> header = Property.of(true);

    @Builder.Default
    @Schema(
        title = "The field separator character"
    )
    private final Property<Character> fieldSeparator = Property.of(',');

    @Builder.Default
    @Schema(
        title = "The text delimiter character"
    )
    private final Property<Character> textDelimiter = Property.of('"');

    @Builder.Default
    @Schema(
        title = "The character used to separate rows"
    )
    private final Property<String> lineDelimiter = Property.of("\n");

    @Builder.Default
    @Schema(
        title = "Whether fields should always be delimited using the textDelimiter option."
    )
    private final Property<Boolean> alwaysDelimitText = Property.of(false);

    @Builder.Default
    @Schema(
        title = "The name of a supported charset"
    )
    private final Property<String> charset = Property.of(StandardCharsets.UTF_8.name());


    @Override
    public Output run(RunContext runContext) throws Exception {
        // temp file
        File tempFile = runContext.workingDir().createTempFile(".csv").toFile();

        // reader
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // formatter
        this.init(runContext);

        try (
            Reader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            Writer fileWriter = new BufferedWriter(new FileWriter(tempFile, Charset.forName(runContext.render(this.charset).as(String.class).orElseThrow())), FileSerde.BUFFER_SIZE);
            de.siegmar.fastcsv.writer.CsvWriter csvWriter = this.csvWriter(fileWriter, runContext)
        ) {

            var headerValue = runContext.render(header).as(Boolean.class).orElseThrow();
            Flux<Object> flowable = FileSerde.readAll(inputStream)
                .doOnNext(new Consumer<>() {
                    private boolean first = false;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void accept(Object row) {
                        if (row instanceof List) {
                            List<Object> casted = (List<Object>) row;

                            if (headerValue) {
                                throw new IllegalArgumentException("Invalid data of type List with header");
                            }

                            var record = casted.stream().map(field -> convert(field)).toList();
                            csvWriter.writeRecord(record);
                        } else if (row instanceof Map) {
                            Map<String, Object> casted = (Map<String, Object>) row;

                            if (!first) {
                                this.first = true;
                                if (headerValue) {
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

    private de.siegmar.fastcsv.writer.CsvWriter csvWriter(Writer writer, RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = de.siegmar.fastcsv.writer.CsvWriter.builder();

        runContext.render(this.textDelimiter).as(Character.class)
            .ifPresent(builder::quoteCharacter);

        runContext.render(this.fieldSeparator).as(Character.class)
            .ifPresent(builder::fieldSeparator);

        runContext.render(this.lineDelimiter).as(String.class)
            .map(LineDelimiter::of)
            .ifPresent(builder::lineDelimiter);

        runContext.render(this.alwaysDelimitText).as(Boolean.class)
            .ifPresent(b -> builder.quoteStrategy(QuoteStrategies.ALWAYS));

        return builder.build(writer);
    }
}
