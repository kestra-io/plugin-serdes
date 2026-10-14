# How to use the Serdes plugin

Convert files between data formats — CSV, JSON, Avro, Parquet, Excel, XML, YAML, Protobuf, and Markdown — from Kestra flows.

## Tasks

All tasks require `from` (a `kestra://` URI pointing to the source file) and return `uri` (the converted file in Kestra's internal storage), except where noted.

### CSV

`csv.CsvToIon` converts a CSV file to ION — set `from`. Control parsing with `header` (default `true`), `fieldSeparator` (default `,`), `textDelimiter` (default `"`), `skipEmptyRows` (default `false`), `skipRows` (default 0), `charset` (default `UTF-8`), and `onBadLines` (default `ERROR`; also `WARN` or `SKIP`). A leading UTF-8 byte-order mark is stripped automatically. Use `onEmptyHeader` (default `DROP`) to drop trailing unnamed header columns, or `RENAME` to keep them with generated names (`col_0`, `col_1`, ...).

`csv.IonToCsv` converts an ION file to CSV — set `from`. Control output with `header` (default `true`), `fieldSeparator` (default `,`), `textDelimiter` (default `"`), `lineDelimiter` (default `\n`), `quoteMode` (`ALWAYS`, `REQUIRED`, or `NON_NUMERIC`), and `charset` (default `UTF-8`). Inherits date/time formatting from the base writer.

### JSON

`json.JsonToIon` converts a JSON or JSONL file to ION — set `from`. Set `newLine: true` (default) for JSONL input.

`json.IonToJson` converts an ION file to JSON — set `from`. Set `newLine: true` (default) to produce JSONL. Set `shouldKeepAnnotations: false` (default) to strip ION annotations.

`json.JsonToJsonl` converts a JSON array file to JSONL — set `from`.

`json.JsonToYaml` converts JSON to YAML — set `from`. Set `jsonl: true` if the input is JSONL (default `false`).

`json.ValidateJsonSchema` validates a JSON document against a JSON Schema and does not produce a file — set `from` (JSON text, a `kestra://` URI, or an object/array/scalar) and exactly one of `schema` (inline text; use `{{ read('path/to/schema.json') }}` for a namespace file) or `registry`. `registry` targets a Confluent-compatible Schema Registry such as Karapace: set `url` plus either `subject` (with `subjectVersion`, default `latest`) or `id`, and use `options.auth` (basic or bearer), `options.ssl`, `options.proxy` and `options.timeout` as needed. Registered schemas must be of type `JSON` and have no `references`. The draft comes from the schema's `$schema`, falling back to `defaultDialect` (default `DRAFT_2020_12`); with draft-04 the validator mirrors java-json-tools/json-schema-validator (same checked `format` values, ECMA-262 regexes, unknown keywords ignored), so set `defaultDialect: DRAFT_4` for schemas written for that library. Under 2020-12 and 2019-09, `format` is annotation-only. By default (`failOnInvalid: true`) the task fails with a summary of the first errors; with `failOnInvalid: false` it succeeds and returns `valid` and `errors` (`instanceLocation` as a JSON Pointer, `keyword`, `message`, `schemaLocation`; capped at 100), plus `schemaId` and `schemaVersion` for registry schemas. Remote `$ref` URLs are never fetched and the data is read as a single document (JSONL is not validated per record).

### Avro

`avro.AvroToIon` converts an Avro file to ION — set `from`. Set `onBadLines` to control error handling (default `ERROR`).

`avro.IonToAvro` converts an ION file to Avro — set `from`. Optionally provide `schema` (Avro schema JSON). Configure type coercion with `trueValues`, `falseValues`, `nullValues`, `decimalSeparator` (default `.`), `strictSchema` (default `false`), and `inferAllFields` (default `false`).

`avro.InferAvroSchemaFromIon` infers an Avro schema from an ION file — set `from`. Control inference with `numberOfRowsToScan` (default 100).

### Parquet

`parquet.ParquetToIon` converts a Parquet file to ION — set `from`.

`parquet.IonToParquet` converts an ION file to Parquet — set `from`. Control output with `compressionCodec` (default `GZIP`; also `UNCOMPRESSED`, `SNAPPY`, `ZSTD`), `parquetVersion` (default `V2`), `rowGroupSize`, `pageSize`, and `dictionaryPageSize`. Inherits Avro type coercion options.

### Excel

`excel.ExcelToIon` converts an Excel file to ION — set `from`. Optionally filter by `sheetsTitle` (list of sheet names to include). Control rendering with `valueRender` (default `UNFORMATTED_VALUE`) and `dateTimeRender` (default `UNFORMATTED_VALUE`). Set `header` (default `true`) and `skipEmptyRows` (default `false`). The output includes `uris` (map of sheet name → ION file URI) and `size`.

`excel.IonToExcel` converts an ION file to Excel — set `from`. Set `sheetsTitle` (default `Sheet`), `header` (default `true`), and `styles` (default `true`). The output includes `uri` and `size`.

### XML

`xml.XmlToIon` converts an XML file to ION — set `from`. Optionally set `query` (XPath selector to extract a subset). Element and attribute text that looks numeric or boolean is coerced to the matching ION type by default, except when a value is out of the finite `double` range (e.g. `25E2568`), which is kept as a string to avoid silently becoming `Infinity`; use `parserConfiguration.forceList`, `forceString`, `keepNumberAsString`, or `keepBooleanAsString` to control list-wrapping and opt individual fields or every field out of that coercion.

`xml.IonToXml` converts an ION file to XML — set `from`. Set `rootName` (default `items`) as the root element name.

### YAML

`yaml.YamlToIon` converts a YAML file to ION — set `from`.

`yaml.IonToYaml` converts an ION file to YAML — set `from`.

### Protobuf

`protobuf.ProtobufToIon` converts a Protobuf binary file to ION — set `from`.

### Markdown

`markdown.MarkdownToHtml` converts a Markdown file to HTML — set `from`.

`markdown.HtmlToMarkdown` converts an HTML file to Markdown — set `from`. Optionally set `ignoreTags` (list of HTML tags to strip) and `baseUri`.

`markdown.MarkdownToText` converts a Markdown file to plain text — set `from`.
