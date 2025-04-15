package io.kestra.plugin.serdes.avro.infer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.kestra.plugin.serdes.avro.InferAvroSchema.mergeTypes;
import static org.apache.avro.Schema.Type.*;
import static org.assertj.core.api.Assertions.assertThat;

public class MergeAvroTypesTest {

    @Test
    void equals() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(STRING))),
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(STRING)))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, STRING);
            });
    }

    @Test
    void mergeTwoSingleSchema() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.create(NULL)),
            new Field(rndStr(), Schema.create(STRING))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, STRING);
            });
    }

    @Test
    void mergeAIsUnion_doesNotContainsB() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(STRING))),
            new Field(rndStr(), Schema.create(INT))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, STRING, INT);
            });
    }

    @Test
    void mergeBIsUnion_doesNotContainsA() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.create(INT)),
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(STRING)))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, STRING, INT);
            });
    }

    @Test
    void mergeAIsUnion_alreadyContainsA() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(INT))),
            new Field(rndStr(), Schema.create(INT))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, INT);
            });
    }

    @Test
    void mergeBIsUnion_alreadyContainsA() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.create(INT)),
            new Field(rndStr(), Schema.createUnion(Schema.create(NULL), Schema.create(INT)))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(Schema.Type.UNION);
                assertThat(schema.getTypes())
                    .extracting(Schema::getType).containsOnly(NULL, INT);
            });
    }

    @Test
    void mergeObjects_with_sameField() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.createRecord(rndStr(), "doc", "namespace", false,
                List.of(new Field("myField", Schema.create(STRING)))
            )),
            new Field(rndStr(), Schema.createRecord(rndStr(), "doc", "namespace", false,
                List.of(new Field("myField", Schema.create(STRING)))
            ))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(RECORD);
                assertThat(schema.getFields())
                    .extracting(Field::name).containsOnly("myField");
            });
    }

    @Test
    void mergeObjects_with_commonField_havingMultipleTypes() {
        assertThat(mergeTypes(
            new Field(rndStr(), Schema.createRecord(rndStr(), "doc", "namespace", false,
                List.of(
                    new Field("fieldOne", Schema.create(NULL)),
                    new Field("fieldTwo", Schema.createUnion(Schema.create(NULL), Schema.create(STRING))),
                    new Field("fieldThree", Schema.createUnion(Schema.create(NULL), Schema.create(INT)))
                )
            )),
            new Field(rndStr(), Schema.createRecord(rndStr(), "doc", "namespace", false,
                List.of(
                    new Field("fieldOne", Schema.create(STRING)),
                    new Field("fieldTwo", Schema.create(STRING)),
                    new Field("fieldThree", Schema.create(STRING))
                )
            ))
        )).extracting(Field::schema)
            .satisfies(schema -> {
                assertThat(schema).extracting(Schema::getType).isEqualTo(RECORD);
                assertThat(schema.getFields())
                    .extracting(Field::name).containsOnly("fieldOne", "fieldTwo", "fieldThree");
                assertThat(schema.getField("fieldOne").schema().getTypes())
                    .extracting(Schema::getType)
                    .containsOnly(NULL, STRING);
                assertThat(schema.getField("fieldTwo").schema().getTypes())
                    .extracting(Schema::getType)
                    .containsOnly(NULL, STRING);
                assertThat(schema.getField("fieldThree").schema().getTypes())
                    .extracting(Schema::getType)
                    .containsOnly(NULL, STRING, INT);
            });
    }

    String rndStr() {
        java.util.Random random = new java.util.Random();
        return "rnd_" + random.nextLong(0, Long.MAX_VALUE);
    }
}
