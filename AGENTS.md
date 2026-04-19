# Kestra Serdes Plugin

## What

- Provides plugin components under `io.kestra.plugin.serdes`.
- Includes classes such as `OnBadLines`, `IonToAvro`, `AvroSchemaValidation`, `AvroToIon`.

## Why

- What user problem does this solve? Teams need to convert data (SerDes) between common formats for Kestra workflows from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Serialization steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Serialization.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `serdes`

### Key Plugin Classes

- `io.kestra.plugin.serdes.avro.AvroToIon`
- `io.kestra.plugin.serdes.avro.InferAvroSchemaFromIon`
- `io.kestra.plugin.serdes.avro.IonToAvro`
- `io.kestra.plugin.serdes.csv.CsvToIon`
- `io.kestra.plugin.serdes.csv.IonToCsv`
- `io.kestra.plugin.serdes.excel.ExcelToIon`
- `io.kestra.plugin.serdes.excel.IonToExcel`
- `io.kestra.plugin.serdes.json.IonToJson`
- `io.kestra.plugin.serdes.json.JsonToIon`
- `io.kestra.plugin.serdes.json.JsonToJsonl`
- `io.kestra.plugin.serdes.json.JsonToToon`
- `io.kestra.plugin.serdes.json.ToonToJson`
- `io.kestra.plugin.serdes.markdown.HtmlToMarkdown`
- `io.kestra.plugin.serdes.markdown.MarkdownToHtml`
- `io.kestra.plugin.serdes.markdown.MarkdownToText`
- `io.kestra.plugin.serdes.parquet.IonToParquet`
- `io.kestra.plugin.serdes.parquet.ParquetToIon`
- `io.kestra.plugin.serdes.protobuf.ProtobufToIon`
- `io.kestra.plugin.serdes.xml.IonToXml`
- `io.kestra.plugin.serdes.xml.XmlToIon`
- `io.kestra.plugin.serdes.yaml.IonToYaml`
- `io.kestra.plugin.serdes.yaml.JsonToYaml`
- `io.kestra.plugin.serdes.yaml.YamlToIon`
- `io.kestra.plugin.serdes.yaml.YamlToJson`

### Project Structure

```
plugin-serdes/
├── src/main/java/io/kestra/plugin/serdes/yaml/
├── src/test/java/io/kestra/plugin/serdes/yaml/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
