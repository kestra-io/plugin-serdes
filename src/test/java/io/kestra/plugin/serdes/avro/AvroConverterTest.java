package io.kestra.plugin.serdes.avro;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.serdes.SerdesUtils;
import io.kestra.plugin.serdes.csv.CsvToIon;
import io.kestra.plugin.serdes.json.JsonToIon;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class AvroConverterTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    void fullCsv() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/full.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .fieldSeparator(Property.ofValue(",".charAt(0)))
            .header(Property.ofValue(true))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        IonToAvro.Output avroRunOutput = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, avroRunOutput.getUri())),
            is(IonToAvroTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                        .getResource("csv/full.avro"))
                    .toURI())))
            )
        );
    }

    @Test
    void fullJson() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/full.jsonl");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .build();
        JsonToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        IonToAvro.Output avroRunOutput = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, avroRunOutput.getUri())),
            is(IonToAvroTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                        .getResource("csv/full.avro"))
                    .toURI())))
            )
        );
    }

    @Test
    void csvStrictSchema() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/strict_schema.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .fieldSeparator(Property.ofValue(",".charAt(0)))
            .header(Property.ofValue(true))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .strictSchema(Property.ofValue(true))
            .build();

        RuntimeException re = assertThrows(RuntimeException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(re.getCause().getClass().getSimpleName(), is("IllegalRow"));
        assertThat(re.getCause().getCause().getClass().getSimpleName(), is("IllegalStrictRowConversion"));
    }

    @Test
    void csvStrictSchemaArray() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/strict_schema_array.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .fieldSeparator(Property.ofValue(",".charAt(0)))
            .header(Property.ofValue(false))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .strictSchema(Property.ofValue(true))
            .build();

        RuntimeException re = assertThrows(RuntimeException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(re.getCause().getClass().getSimpleName(), is("IllegalRow"));
        assertThat(re.getCause().getCause().getClass().getSimpleName(), is("IllegalStrictRowConversion"));
    }

    @Test
    void csvNoStrictSchema() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/strict_schema.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .fieldSeparator(Property.ofValue(",".charAt(0)))
            .header(Property.ofValue(true))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        // No exception should be thrown
        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }

    @Test
    void jsonStrictSchema() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/strict_schema.jsonl");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .build();
        JsonToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .strictSchema(Property.ofValue(true))
            .build();

        RuntimeException re = assertThrows(RuntimeException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(re.getCause().getClass().getSimpleName(), is("IllegalRow"));
        assertThat(re.getCause().getCause().getClass().getSimpleName(), is("IllegalStrictRowConversion"));
    }

    @Test
    void jsonStrictSchemaNested() throws Exception {
        String read = SerdesUtils.readResource("csv/nested.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/strict_schema_nested.jsonl");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .build();
        JsonToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .strictSchema(Property.ofValue(true))
            .build();

        RuntimeException re = assertThrows(RuntimeException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(re.getCause().getClass().getSimpleName(), is("IllegalRow"));
        assertThat(re.getCause().getCause().getClass().getSimpleName(), is("IllegalRowConvertion"));
        assertThat(re.getCause().getCause().getCause().getClass().getSimpleName(), is("IllegalCellConversion"));
        assertThat(re.getCause().getCause().getCause().getCause().getClass().getSimpleName(), is("IllegalStrictRowConversion"));
    }

    @Test
    void jsonNoAliases() throws Exception {
        String read = SerdesUtils.readResource("csv/portfolio_without_aliases.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/portfolio.json");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .newLine(Property.ofValue(Boolean.FALSE))
            .build();
        JsonToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        RuntimeException spe = assertThrows(ConstraintViolationException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(spe.getMessage(), containsString("Illegal character in: nbJH_DTP-Helpdesk"));
    }

    @Test
    void jsonAliases() throws Exception {
        String read = SerdesUtils.readResource("csv/portfolio_aliases.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/portfolio.json");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        JsonToIon reader = JsonToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .newLine(Property.ofValue(Boolean.FALSE))
            .build();
        JsonToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        IonToAvro.Output avroRunOutput = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
            IonToAvroTest.avroSize(this.storageInterface.get(TenantService.MAIN_TENANT, null, avroRunOutput.getUri())),
            is(IonToAvroTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(IonToAvroTest.class.getClassLoader()
                        .getResource("csv/portfolio_aliases.avro"))
                    .toURI())))
            )
        );

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(this.storageInterface.get(TenantService.MAIN_TENANT, null, avroRunOutput.getUri()), datumReader);
        dataFileReader.forEach(genericRecord -> {
            GenericRecord scenario = ((GenericRecord) ((GenericRecord) genericRecord.get("it")).get("selectedScenario"));
            assertThat(scenario.get("nbJH_DTP_Sales"), notNullValue());
            assertThat(scenario.get("nbJH_DTP_Cloud_Connectivity"), notNullValue());
            assertThat(scenario.get("nbJH_DTP_Helpdesk"), notNullValue());
        });
    }

    @Test
    void rowWithMissingFieldsAndGoodSeparator() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/row_with_missing_fields_and_good_separator.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvToIon reader = CsvToIon.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvToIon.class.getName())
            .from(Property.ofValue(csv.toString()))
            .fieldSeparator(Property.ofValue(",".charAt(0)))
            .header(Property.ofValue(false))
            .build();
        CsvToIon.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(runContextFactory, reader, ImmutableMap.of()));

        IonToAvro task = IonToAvro.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(IonToAvro.class.getName())
            .from(Property.ofValue(readerRunOutput.getUri().toString()))
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        RuntimeException re = assertThrows(RuntimeException.class, () -> {
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        assertThat(re.getMessage(), containsString("on cols with data [null] and schema [\"double\"] on field 'double' with data "));
        assertThat(re.getCause().getClass().getSimpleName(), is("IllegalRow"));
        assertThat(re.getCause().getCause().getCause().getCause().getClass().getSimpleName(), is("NullPointerException"));
    }

    public static class Utils {
        public static void oneField(Object v, Object expected, Schema type, Boolean inferAllFields) throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
            oneField(AvroConverter.builder().inferAllFields(inferAllFields).build(), v, expected, type);
        }

        public static void oneField(AvroConverter avroConverter, Object v, Object expected, Schema type) throws AvroConverter.IllegalRowConvertion, AvroConverter.IllegalStrictRowConversion {
            Schema schema = oneFieldSchema(type);

            HashMap<String, Object> map = new HashMap<>();
            map.put("fieldName", v);

            GenericData.Record record = avroConverter.fromMap(schema, map);
            GenericRecord serialized = Utils.test(schema, record);

            assertThat(record, is(serialized));
            assertThat(serialized.get("fieldName"), is(expected));
        }

        public static void oneFieldFailed(Object v, Schema type, Boolean inferAllFields) {
            AvroConverter avroConverter = AvroConverter.builder().inferAllFields(inferAllFields).build();
            Schema schema = oneFieldSchema(type);

            assertThrows(AvroConverter.IllegalRowConvertion.class, () -> avroConverter.fromMap(schema, ImmutableMap.of("fieldName", v)));
        }

        public static Schema oneFieldSchema(Schema type) {
            return schema(a -> a.name("fieldName").type(type).noDefault());
        }

        public static Schema schema(Consumer<SchemaBuilder.FieldAssembler<Schema>> consumer) {
            SchemaBuilder.FieldAssembler<Schema> b = SchemaBuilder.record("rGenericDatumWriterecordName")
                .fields();

            consumer.accept(b);

            return b.endRecord();
        }

        public static GenericRecord test(Schema schema, GenericData.Record record) {
            try {
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema, AvroConverter.genericData());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

                writer.write(record, encoder);
                encoder.flush();

                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, AvroConverter.genericData());
                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
