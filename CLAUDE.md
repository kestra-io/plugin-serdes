# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kestra plugin for serialization/deserialization (SerDes) between common data formats (JSON, CSV, Avro, Parquet, Excel, Protobuf, XML, YAML, Markdown). All conversions flow through Amazon ION as an intermediate format: **Source → ION → Target**.

- **Language:** Java 21
- **Build:** Gradle 9.3.1 (single module), uses `io.kestra:platform` BOM for dependency management
- **DI Framework:** Micronaut (not Spring)
- **Annotations:** Lombok (`@SuperBuilder`, `@Getter`, `@NoArgsConstructor`, `@ToString`, `@EqualsAndHashCode`)

## Build & Test Commands

```bash
./gradlew test          # Build and run all tests (always run after code changes)
./gradlew build         # Full build
./gradlew check         # Run all checks
./gradlew test --tests "io.kestra.plugin.serdes.csv.CsvToIonTest"           # Single test class
./gradlew test --tests "io.kestra.plugin.serdes.csv.CsvToIonTest.testName"  # Single test method
```

## Architecture

### Package Structure

Each format lives under `io.kestra.plugin.serdes.<format>` with task classes following the naming pattern `<Format>ToIon` (reader) and `IonTo<Format>` (writer). Each package has a `package-info.java` with `@PluginSubGroup` metadata.

### Task Implementation Pattern

All tasks extend `Task`, implement `RunnableTask<T.Output>`, and follow this structure:
- All input properties use `Property<T>` (supports `{{ expression }}` rendering)
- Rendered property variables are prefixed with `r` (e.g., `rEndpoint`, not `renderedEndpoint`)
- Mandatory properties annotated with `@NotNull`
- `@Schema` annotations on all properties and Output fields
- `@Plugin(examples = ...)` with `@Example(full = true)` showing complete runnable flows
- Output is a static inner class implementing `io.kestra.core.models.tasks.Output`
- File I/O through `runContext.storage()`, always return `URI` (never `File`)
- Metrics via `runContext.metric(Counter.of(...))`, each needs `@Metric` annotation on `@Plugin`

### Key Base Classes

- `AbstractTextWriter` — date/time formatting for text-based conversions
- `AbstractAvroConverter` — schema-aware Avro conversions with inference support

## Testing

- **Framework:** JUnit 5 with `@KestraTest` annotation
- **Injection:** Micronaut `@Inject` for `RunContextFactory`, `StorageInterface`, etc.
- **Test execution:** `TestsUtils.mockRunContext(runContextFactory, task, Map.of())`
- **Sanity checks:** YAML flow definitions in `src/test/resources/sanity-checks/`, tested with `@ExecuteFlow` annotation and `@KestraTest(startRunner = true)`
- **Assertion library:** Hamcrest

## Conventions (from AGENTS.md)

- **TDD for bug fixes:** Write failing test first, then fix. Mandatory.
- **Conventional commits:** `feat(csv):`, `fix(xml):`, `docs:`, `chore:`
- **Surgical changes only:** Touch only what is necessary, no opportunistic refactoring
- **Preserve existing comments** unless improving clarity
- **Java style:** Use `var` for local variables, `getFirst()` instead of `get(0)`, prefer `.toList()` over `collect(Collectors.toList())`, no unused imports
- **Documentation:** Use Java multiline string blocks (`"""`) for `@Schema` and `@Example`. Examples must be full runnable flows with flow id and namespace. Do not specify `lang = "yaml"` in `@Example`.
- **No silent breaking changes**, no bypassing Kestra abstractions

## Editor Config

4-space indent for Java, 2-space for YAML/JSON. UTF-8, LF line endings, trim trailing whitespace.
