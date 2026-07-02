package io.kestra.plugin.serdes.json;

import com.fasterxml.jackson.databind.ObjectReader;
import io.kestra.core.preview.FilePreview;
import io.kestra.core.preview.FileRenderer;
import io.kestra.core.serializers.JacksonMapper;
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
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "JSONL file renderer",
    description = """
        Preview JSONL (newline-delimited JSON) files inside the Kestra UI. Each line \
        is parsed as a standalone JSON object and rendered as a row."""
)
public class JsonlFileRenderer implements FileRenderer {
    @Override
    public boolean supports(String extension) {
        return "jsonl".equalsIgnoreCase(extension);
    }

    @Override
    public FilePreview render(String extension, InputStream inputStream, Optional<Charset> charset, int maxRows) throws IOException {
        if (!supports(extension)) {
            throw new IllegalArgumentException("Unsupported extension: " + extension);
        }

        List<Object> records = new ArrayList<>();
        boolean truncated = false;

        ObjectReader objectReader = JacksonMapper.ofJson().readerFor(Object.class);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset.orElse(StandardCharsets.UTF_8)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                if (records.size() >= maxRows) {
                    truncated = true;
                    break;
                }
                records.add(objectReader.readValue(line));
            }
        }

        return FilePreview.builder()
            .content(records)
            .truncated(truncated)
            .extension(extension)
            .type(FilePreview.Type.LIST)
            .build();
    }
}
