package io.kestra.plugin.serdes.xml;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read a xml file and write it to an ion serialized data file."
)
public class XmlReader extends Task implements RunnableTask<XmlReader.Output> {
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
    private final String charset = StandardCharsets.UTF_8.name();

    @Schema(
        title = "The name of a supported charset",
        description = "Default value is UTF-8."
    )
    private String query;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // reader
        URI from = new URI(runContext.render(this.from));

        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from), charset));
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            JSONObject jsonObject = XML.toJSONObject(input);
            Object result = result(jsonObject);

            if (result instanceof JSONObject) {
                Map<String, Object> map = ((JSONObject) result).toMap();
                FileSerde.write(output, ((JSONObject) result).toMap());
                runContext.metric(Counter.of("records", map.size()));
            } else if (result instanceof JSONArray) {
                List<Object> list = ((JSONArray) result).toList();
                list.forEach(throwConsumer(o -> {
                    FileSerde.write(output, o);
                }));
                runContext.metric(Counter.of("records", list.size()));
            } else {
                FileSerde.write(output, result);
            }

            output.flush();
        }

        return Output
            .builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    private Object result(JSONObject jsonObject) {
        if (this.query != null) {
            return jsonObject.query(this.query);
        } else {
            return  jsonObject;
        }
    }
//
//    private Object write(RunContext runContext, Object result, OutputStream output) {
//        if (result instanceof JSONArray) {
//            List<Object> list = ((JSONArray) result).toList();
//            list.forEach(throwConsumer(o -> {
//                FileSerde.write(output, o);
//            }));
//            runContext.metric(Counter.of("records", list.size()));
//        }
//    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a temporary result file"
        )
        private final URI uri;
    }
}
