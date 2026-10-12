package io.kestra.plugin.serdes.avro.infer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class InferAvroSchemaFromIonTest {
    @Test
    void simple_string_should_be_optional() throws IOException {
        this.run(
            """
                {myString: "hello"}
                """,
            """
                {"fields": [{"name": "myString", "type": ["null","string"]}]}
                """
        );
    }

    @Test
    void array_of_strings() throws IOException {
        // Root arrays are wrapped in a record because Avro requires a RECORD at the top level.
        this.run(
            """
                ["one", "two", "three"]
                """,
            """
                {
                  "type": "record",
                  "name": "root",
                  "fields": [
                    {
                      "name": "value",
                      "type": {"type": "array", "items": ["null", "string"]}
                    }
                  ]
                }
                """
        );
    }

    @Test
    void complex_nested_records_with_clashing_field_names_do_not_raise_error() throws IOException {
        this.run(
            """
                {
                    myField: "hey",
                    firstObject: {
                        myField: {
                            myNestedField: "heya"
                        },
                        myNestedObject: {
                            myField: "heyo"
                        }
                    },
                    secondObject: {
                        myField: "heyu"
                    }
                }
                """,
            """
                { }
                """
        );
    }

    @Test
    void array_of_objects() throws IOException {
        this.run(
            """
                {
                    myArray: [
                        {myName: "one"},
                        {myName: "two"}
                    ]
                }
                """,
            """
                {
                   "fields": [
                     {
                       "name": "myArray",
                       "type": [
                         {
                           "type": "array",
                           "items": [
                             {
                               "type": "record",
                               "name": "myArray_items",
                               "fields": [
                                 {
                                   "name": "myName",
                                   "type": [
                                     "string",
                                     "null"
                                   ]
                                 }
                               ]
                             },
                             "null"
                           ]
                         },
                         "null"
                       ]
                     }
                   ]
                 }
                """
        );
    }

    @Test
    void field_names_with_special_characters_should_be_sanitized() throws IOException {
        this.run(
            """
                {"fichier-code-nature-voie-ficoba3-20230811": "hello"}
                """,
            """
                {"fields": [{"name": "fichier_code_nature_voie_ficoba3_20230811", "type": ["null","string"]}]}
                """
        );
    }

    @Test
    void field_names_starting_with_digit_should_be_sanitized() throws IOException {
        this.run(
            """
                {"123field": "hello"}
                """,
            """
                {"fields": [{"name": "_123field", "type": ["null","string"]}]}
                """
        );
    }

    @Test
    void field_names_with_special_characters_should_not_collide() throws IOException {
        this.run(
            """
                {
                    "foo-bar": "one",
                    "foo_bar": "two"
                }
                """,
            """
                {
                  "fields": [
                    {"name": "foo_bar", "type": ["null","string"]},
                    {"name": "foo_bar_1", "type": ["null","string"]}
                  ]
                }
                """
        );
    }

    @Test
    void empty_field_name_should_produce_valid_placeholder() throws IOException {
        this.run(
            """
                {"": "hello"}
                """,
            """
                {"fields": [{"name": "_", "type": ["null","string"]}]}
                """
        );
    }

    @Test
    void nested_record_with_special_characters() throws IOException {
        this.run(
            """
                {"outer-key": {"inner.key": "value"}}
                """,
            """
                {
                  "fields": [{
                    "name": "outer_key",
                    "type": [{
                      "type": "record",
                      "name": "outer_key",
                      "fields": [{"name": "inner_key", "type": ["null","string"]}]
                    }, "null"]
                  }]
                }
                """
        );
    }

    @Test
    void multi_row_key_order_produces_stable_names() throws IOException {
        // Even when key iteration order differs between rows, the same original
        // key must always map to the same sanitized name.
        var output = new ByteArrayOutputStream();
        new InferAvroSchema(2).inferAvroSchemaFromIon(
            new ByteArrayInputStream(
                """
                    {"foo-bar": "a", "foo_bar": "b"}
                    {"foo_bar": "c", "foo-bar": "d"}
                    """.getBytes()),
            output
        );
        var schema = new String(output.toByteArray());
        JSONAssert.assertEquals(
            """
                {
                  "fields": [
                    {"name": "foo_bar", "type": ["null","string"]},
                    {"name": "foo_bar_1", "type": ["null","string"]}
                  ]
                }
                """,
            schema,
            JSONCompareMode.LENIENT
        );
    }

    @Test
    void array_of_objects_with_unmatching_types() throws IOException {
        this.run(
            """
                {
                    myArray: [
                        {myName: "one"},
                        {myName: "two", anAdditionalField: "hey"},
                        {myName: "three"}
                    ]
                }
                """,
            """
                {
                  "fields": [
                      {
                        "name": "myArray",
                        "type": [
                          {
                            "type": "array",
                            "items": [
                              "null",
                              {
                                "type": "record",
                                "fields": [
                                  {
                                    "name": "myName",
                                    "type": [
                                      "null",
                                      "string"
                                    ]
                                  },
                                  {
                                    "name": "anAdditionalField",
                                    "type": [
                                      "string",
                                      "null"
                                    ]
                                  }
                                ]
                              }
                            ]
                          },
                          "null"
                        ]
                      }
                    ]
                }
                """
        );
    }

    void run(String input, String expected) throws IOException {
        var output = new ByteArrayOutputStream();

        // when
        new InferAvroSchema().inferAvroSchemaFromIon(
            new ByteArrayInputStream(input.getBytes()),
            output
        );

        // then
        try (ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray())) {
            var resultingAvroSchema = new String(in.readAllBytes());
            JSONAssert.assertEquals(
                expected,
                resultingAvroSchema,
                JSONCompareMode.LENIENT
            );
        }
    }
}
