/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InferSchemaTest extends AbstractIntegrationTest {

    /**
     * Verifies that the Spark schema constructed by a columnInfo response (which contains an element
     * for every possible TDE type) is correct. Main purpose of the test is to detect changes in /v1/rows
     * that could affect how the Spark schema is constructed.
     *
     * @throws Exception
     */
    @Test
    void inferFromAllTdeTypes() throws Exception {
        String columnInfoResponse = readClasspathFile("allTypes-columnInfo-response.txt");
        StructType schema = SchemaInferrer.inferSchema(columnInfoResponse);

        assertEquals(36, schema.size(), "The TDE has 36 columns, and the hidden 'rowid' column that's returned " +
            "by /v1/rows should not be included in the Spark schema, as it will never be populated with a value.");

        String actualJson = schema.prettyJson();
        String expectedJson = readClasspathFile("allTypes-expected-spark-schema.json");
        JSONAssert.assertEquals(expectedJson, actualJson, true);
    }
}
