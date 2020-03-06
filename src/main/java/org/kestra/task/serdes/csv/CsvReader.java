package org.kestra.task.serdes.csv;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvRow;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.serdes.serializers.ObjectsSerde;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Read a csv file and write it to a java serialized data file."
)
public class CsvReader extends Task implements RunnableTask<CsvReader.Output> {
    @NotNull
    @InputProperty(
        description = "Source file URI"
    )
    private String from;

    @Builder.Default
    @InputProperty(
        description = "Specifies if the first line should be the header (default: false)"
    )
    private Boolean header = true;

    @Builder.Default
    @InputProperty(
        description = "The field separator character (default: ',' - comma)"
    )
    private Character fieldSeparator = ",".charAt(0);

    @Builder.Default
    @InputProperty(
        description = "The text delimiter character (default: '\"' - double quotes)"
    )
    private Character textDelimiter = "\"".charAt(0);

    @Builder.Default
    @InputProperty(
        description = "Specifies if empty rows should be skipped (default: true)"
    )
    private Boolean skipEmptyRows = false;

    @Builder.Default
    @InputProperty(
        description = "Number of lines to skip at the start of the file"
    )
    private Integer skipRows = 0;

    @Builder.Default
    @InputProperty(
        description = "The name of a supported charset",
        body = "Default value is UTF-8."
    )
    private String charset = StandardCharsets.UTF_8.name();

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));
        de.siegmar.fastcsv.reader.CsvReader csvReader = this.csvReader();

        // temp file
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".javas");

        try (
            CsvParser csvParser = csvReader.parse(new InputStreamReader(runContext.uriToInputStream(from), charset));
            ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(tempFile))
        ) {
            CsvRow row;
            int count = 0;
            int skipped = 0;

            while ((row = csvParser.nextRow()) != null) {
                if (this.skipRows > 0 && skipped < this.skipRows) {
                    skipped++;
                } else {
                    Object map;
                    count++;

                    if (header) {
                        map = row.getFieldMap();
                    } else {
                        map = row.getFields();
                    }

                    ObjectsSerde.write(output, map);
                }
            }

            runContext.metric(Counter.of("records", count));
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "URI of a temporary result file"
        )
        private URI uri;
    }

    private de.siegmar.fastcsv.reader.CsvReader csvReader() {
        de.siegmar.fastcsv.reader.CsvReader csvReader = new de.siegmar.fastcsv.reader.CsvReader();

        if (this.header != null) {
            csvReader.setContainsHeader(this.header);
        }

        if (this.textDelimiter != null) {
            csvReader.setTextDelimiter(textDelimiter);
        }

        if (this.fieldSeparator != null) {
            csvReader.setFieldSeparator(fieldSeparator);
        }

        if (this.skipEmptyRows != null) {
            csvReader.setSkipEmptyRows(skipEmptyRows);
        }

        return csvReader;
    }
}
