package org.kestra.task.serdes.avro;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.serdes.SerdesUtils;
import org.kestra.task.serdes.csv.CsvReader;
import org.kestra.task.serdes.json.JsonReader;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Consumer;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class AvroConverterTest {
    @Inject
    ApplicationContext applicationContext;

    @Inject
    StorageInterface storageInterface;

    @Inject
    SerdesUtils serdesUtils;

    @Test
    void fullCsv() throws Exception {
        String read = SerdesUtils.readResource("csv/full.avsc");

        File sourceFile = SerdesUtils.resourceToFile("csv/full.csv");
        URI csv = this.serdesUtils.resourceToStorageObject(sourceFile);

        CsvReader reader = CsvReader.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(CsvReader.class.getName())
            .from(csv.toString())
            .fieldSeparator(",".charAt(0))
            .header(true)
            .build();
        CsvReader.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(applicationContext, reader, ImmutableMap.of()));

        AvroWriter task = AvroWriter.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        AvroWriter.Output avroRunOutput = task.run(TestsUtils.mockRunContext(applicationContext, task, ImmutableMap.of()));

        assertThat(
            AvroWriterTest.avroSize(this.storageInterface.get(avroRunOutput.getUri())),
            is(AvroWriterTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader()
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

        JsonReader reader = JsonReader.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(JsonReader.class.getName())
            .from(csv.toString())
            .build();
        JsonReader.Output readerRunOutput = reader.run(TestsUtils.mockRunContext(applicationContext, reader, ImmutableMap.of()));

        AvroWriter task = AvroWriter.builder()
            .id(AvroConverterTest.class.getSimpleName())
            .type(AvroWriter.class.getName())
            .from(readerRunOutput.getUri().toString())
            .schema(read)
            .dateFormat("yyyy/MM/dd")
            .timeFormat("H:mm")
            .build();

        AvroWriter.Output avroRunOutput = task.run(TestsUtils.mockRunContext(applicationContext, task, ImmutableMap.of()));

        assertThat(
            AvroWriterTest.avroSize(this.storageInterface.get(avroRunOutput.getUri())),
            is(AvroWriterTest.avroSize(
                new FileInputStream(new File(Objects.requireNonNull(AvroWriterTest.class.getClassLoader()
                    .getResource("csv/full.avro"))
                    .toURI())))
            )
        );
    }

    public static class Utils {
        public static void oneField(Object v, Object expected, Schema type) throws AvroConverter.IllegalRowConvertion {
            AvroConverter avroConverter = AvroConverter.builder().build();
            Schema schema = oneFieldSchema(type);

            HashMap<String, Object> map = new HashMap<>();
            map.put("fieldName", v);

            GenericData.Record record = avroConverter.fromMap(schema, map);
            GenericRecord serialized = Utils.test(schema, record);

            assertThat(record, is(serialized));
            assertThat(serialized.get("fieldName"), is(expected));
        }

        public static void oneFieldFailed(Object v, Schema type) {
            AvroConverter avroConverter = AvroConverter.builder().build();
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
