# Kestra Serdes Plugin

## What

Serialize and deserialize data formats in Kestra workflows. Exposes 24 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Serialization, allowing orchestration of Serialization-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
