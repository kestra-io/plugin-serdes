package io.kestra.plugin.serdes.csv;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import de.siegmar.fastcsv.reader.CsvRecordHandler;
import io.kestra.core.preview.FilePreview;
import io.kestra.core.preview.FileRenderer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "CSV file renderer",
    description = """
        Preview CSV files inside the Kestra UI. The first row is treated as the header, \
        each following row is rendered as a record mapping header names to values."""
)
public class CsvFileRenderer implements FileRenderer {
    @Override
    public boolean supports(String extension) {
        return "csv".equalsIgnoreCase(extension);
    }

    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        List<Object> records = new ArrayList<>();
        boolean truncated;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset.orElse(StandardCharsets.UTF_8)))) {
            CsvReader<CsvRecord> csvReader = CsvReader.builder().build(CsvRecordHandler.builder().build(), reader);
            Iterator<CsvRecord> iterator = csvReader.iterator();

            List<String> headers = new ArrayList<>();
            if (iterator.hasNext()) {
                headers.addAll(iterator.next().getFields());
            }

            while (iterator.hasNext() && records.size() < maxRows) {
                CsvRecord record = iterator.next();
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    row.put(headers.get(i), i < record.getFieldCount() ? record.getField(i) : null);
                }
                records.add(row);
            }

            truncated = iterator.hasNext();
        }

        return FilePreview.builder()
            .content(records)
            .truncated(truncated)
            .extension(extension)
            .type(FilePreview.Type.LIST)
            .build();
    }
}
