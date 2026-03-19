package io.kestra.plugin.serdes.avro.infer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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
            new InputStreamReader(new ByteArrayInputStream(input.getBytes())),
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
