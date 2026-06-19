package io.kestra.plugin.serdes.avro.converter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import io.kestra.plugin.serdes.avro.AvroConverter;
import io.kestra.plugin.serdes.avro.AvroConverterTest;

/**
 * The formatters are built once per converter instance and reused across rows.
 * Converting more than one row through the same instance with custom formats
 * documents that the cached formatter stays correct for every row.
 */
public class FormatterCachingTest {

    @Test
    void reusesDateFormatterAcrossRows() throws Exception {
        AvroConverter converter = AvroConverter.builder()
            .dateFormat("d/M/yy")
            .build();

        Schema schema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

        AvroConverterTest.Utils.oneField(converter, "28/5/20", LocalDate.of(2020, 5, 28), schema);
        AvroConverterTest.Utils.oneField(converter, "1/6/20", LocalDate.of(2020, 6, 1), schema);
    }

    @Test
    void reusesTimeFormatterAcrossRows() throws Exception {
        AvroConverter converter = AvroConverter.builder()
            .timeFormat("HH:mm:ss")
            .build();

        Schema schema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));

        AvroConverterTest.Utils.oneField(converter, "13:45:30", LocalTime.of(13, 45, 30), schema);
        AvroConverterTest.Utils.oneField(converter, "06:07:08", LocalTime.of(6, 7, 8), schema);
    }

    @Test
    void reusesDatetimeFormatterAcrossRows() throws Exception {
        AvroConverter converter = AvroConverter.builder()
            .datetimeFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
            .build();

        Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

        Instant first = ZonedDateTime.parse("2024-01-15T13:45:30+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant();
        Instant second = ZonedDateTime.parse("2024-02-20T06:07:08+02:00", DateTimeFormatter.ISO_DATE_TIME).toInstant();

        AvroConverterTest.Utils.oneField(converter, "2024-01-15T13:45:30+02:00", first, schema);
        AvroConverterTest.Utils.oneField(converter, "2024-02-20T06:07:08+02:00", second, schema);
    }
}
