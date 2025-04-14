package io.kestra.plugin.serdes.avro;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class InferAvroSchemaFromIonTest {
    private record TestCase(String name, String input, String expected) {
        @Override
        public String toString() {
            return name + ": input" + input;
        }
    }// TODO see to switch out from ParameterizedTest and just use method

    static List<TestCase> testCases = List.of(
        new TestCase(
            "simple string should be optional",
            """
                {myString: "hello"}
                """,
            """
                {"fields": [{"name": "myString", "type": ["null","string"]}]}
                """
        ),
        new TestCase(
            "arrays of string",
            """
                ["one", "two", "three"]
                """,
            """
                {"type": "array", "items": [ "null", "string" ]}
                """
        ),
        new TestCase(
            "complex nested records with clashing field names",
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
                {"TODO": "todo"}
                """
        ),
        new TestCase(
            "array of objects",
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
                        "name" : "myArray",
                        "type" : {
                          "type" : "array",
                          "items" : {
                            "type" : "record",
                            "name" : "myArray_items",
                            "fields" : [
                              {
                                "name" : "myName",
                                "type" : [ "null", "string" ]
                              }
                            ]
                          }
                        }
                    }
                  ]
                }
                """
        )
        /*, new TestCase( TODO unhandled yet, will need to go through all list
            "array of objects with unmatching types",
            """
                {
                    myArray: [
                        {myName: "one"},
                        {myName: "two"},
                        {myName: "three", anAdditionalField: "hey"}
                    ]
                }
                """,
            """
                {
                  "fields": [
                    {
                        "name" : "myArray",
                        "type" : {
                          "type" : "array",
                          "items" : {
                            "type" : "record",
                            "name" : "myArray_items",
                            "fields" : [
                              {
                                "name" : "myName",
                                "type" : [ "null", "string" ]
                              },
                              {
                                "name" : "anAdditionalField",
                                "type" : [ "null", "string" ]
                              }
                            ]
                          }
                        }
                    }
                  ]
                }
                """
        )*/
    );

    @ParameterizedTest
    @FieldSource("testCases")
    void ok(TestCase testCase) throws IOException {
        var output = new ByteArrayOutputStream();

        // when
        InferAvroSchema.inferAvroSchemaFromIon(
            new InputStreamReader(new ByteArrayInputStream(testCase.input().getBytes())),
            output
        );

        // then
        try (ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray())) {
            var resultingAvroSchema = new String(in.readAllBytes());
            JSONAssert.assertEquals(
                testCase.expected(),
                resultingAvroSchema,
                false
            );
        }
    }
}
