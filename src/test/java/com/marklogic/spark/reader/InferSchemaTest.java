/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InferSchemaTest extends AbstractIntegrationTest {

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

        assertEquals(35, schema.size(), "The TDE has 35 columns, and the hidden 'rowid' column that's returned " +
            "by /v1/rows should not be included in the Spark schema, as it will never be populated with a value.");

        String actualJson = schema.prettyJson();
        String expectedJson = readClasspathFile("allTypes-expected-spark-schema.json");
        JSONAssert.assertEquals(expectedJson, actualJson, true);
    }
}
