package io.kestra.plugin.serdes.avro.infer.fullconversion;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@KestraTest
public abstract class FullIonConversionAbstractTest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    RunContextFactory runContextFactory;

    @Disabled("not sure our parser handles a root array, rework needed to make this work")
    @Test
    void JSONArray() throws Exception {
        var input = """
            ["one", "two"]
            """;
        this.run(input, input);
    }

    @Test
    void JSONObject() throws Exception {
        var input = """
            {"hello": "world"}
            """;
        this.run(input, input);
    }

    @Test
    void real() throws Exception {
        this.run( // this type conversion could obviously be improved
            """
            {"hello": 3.14}
            """,
            """
            {"hello": "3.14"}
            """);
    }

    @Test
    void allJSONTypes() throws Exception {
        var input = """
                {
                  "Actors": [
                    {
                      "name": "Tom Cruise",
                      "age": 56,
                      "BornAt": "Syracuse, NY",
                      "Birthdate": "July 3, 1962",
                      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
                      "wife": null,
                      "hasChildren": true,
                      "hasGreyHair": false,
                      "children": [
                        "Suri",
                        "Isabella Jane",
                        "Connor"
                      ]
                    },
                    {
                      "name": "Robert Downey Jr.",
                      "age": 53,
                      "BornAt": "New York City, NY",
                      "Birthdate": "April 4, 1965",
                      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
                      "wife": "Susan Downey",
                      "hasChildren": true,
                      "hasGreyHair": false,
                      "children": [
                        "Indio Falconer",
                        "Avri Roel",
                        "Exton Elias"
                      ]
                    }
                  ]
                }
            """;
        this.run(input, input);
    }

    @Test
    void primitiveTypes() throws Exception {
        this.run(
            """
            {
                Int:1,
                String:"my string1",
                Float:-0.12e4,
                Decimal:7.47
            }
            """,
            """
           {
                Int:1,
                String:"my string1",
                Float:-0.12e4,
                Decimal:"7.47"
            }
           """);
    }

    @Test
    void allData_matching() throws Exception {
        var input = """
            {order_id:1,str1:"my string1",customer_name:null,customer_email:"jenniferschneider@example.com",product_id:11,price:166.88999938964844e0,quantity:1,total:166.88999938964844e0}
            {order_id:2,str1:"my string2",customer_name:null,customer_email:"22222222222222222@example.com",product_id:22,price:122.88999938964844e0,quantity:2,total:222.88999938964844e0}
            """;
        this.run(input, input);
    }

    @Test
    void someData_missingFields() throws Exception {
        this.run("""
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """,
            """
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {id:null, str1:null, labels:null, data1: null}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """
        );
    }

    @Test
    void someData_missingFields_forFirstRow_requiringFullSearch() throws Exception {
        this.run("""
                {}
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """,
            """
                {id:null, str1:null, labels:null, data1: null}
                {id:1, str1:"my string1", labels:["lab1"], data1: {field1: "hey"}}
                {id:2, str1:"my string2", labels:["lab2"], data1: {field1: "heya"}}
                """
        );
    }

    abstract void run(String ionInput, String expectedOutputIon) throws Exception;
}
