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

import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsTest extends AbstractIntegrationTest {

    private static final String XML_DIRECTORY = "/xml-split-test/";
    private static final String XML_COLLECTION = "xml-split-test";
    private static final String XML_SUFFIX = ".xml";
    @Test
    void readXmlTest() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_XML_FILE, "src/test/resources/employee.xml")
            .load();

        assertNotNull(dataset);
        List<Row> rows = dataset.collectAsList();
        assertEquals(3, rows.size());

        dataset
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, XML_COLLECTION)
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
            .option(Options.WRITE_URI_PREFIX, XML_DIRECTORY)
            .option(Options.WRITE_URI_SUFFIX, XML_SUFFIX)
            .save();

        verifyThreeDocsWereWritten();
    }

    private void verifyThreeDocsWereWritten() {
        final int expectedCollectionSize = 3;
        String uri = getUrisInCollection(XML_COLLECTION, expectedCollectionSize).get(0);
        assertTrue(uri.startsWith(XML_DIRECTORY), "URI should start with '" + XML_DIRECTORY + "' due to uriPrefix option: " + uri);
        assertTrue(uri.endsWith(".xml"), "URI should end with '.xml' due to uriSuffix option: " + uri);

        PermissionsTester perms = readDocumentPermissions(uri);
        perms.assertReadPermissionExists("spark-user-role");
        perms.assertUpdatePermissionExists("spark-user-role");
    }

    @Test
    void validPartitionCountAndBatchSize() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, "3")
            .option(Options.READ_BATCH_SIZE, "10000")
            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Expecting all 15 rows to be returned from the view");

        rows.forEach(row -> {
            long id = row.getLong(0);
            assertEquals(id, (long) row.getAs("Medical.Authors.CitationID"), "Verifying that the first column has " +
                "the citation ID and can be accessed via a fully qualified column name.");

            assertTrue(id >= 1 && id <= 5, "The citation ID is expected to be the first column value, and based on our " +
                "test data, it should have a value from 1 to 5; actual value: " + id);

            assertEquals(9, row.size(), "Expecting the row to have 9 columns since the TDE defines 9 columns, and the " +
                "Spark schema is expected to be inferred from the TDE.");
        });
    }

    @Test
    void emptyQualifier() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', '')")
            .load()
            .collectAsList();

        assertEquals(15, rows.size());

        rows.forEach(row -> {
            long id = row.getLong(0);
            assertEquals(id, (long) row.getAs("CitationID"), "Verifying that the first column has " +
                "the citation ID and can be accessed via a column name with no qualifier.");
        });
    }

    @Test
    void queryReturnsZeroRows() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .collectAsList();

        assertEquals(0, rows.size(), "When no rows exist, the /v1/rows endpoint currently (as of 11.0) throws a " +
            "fairly cryptic error. A user does not expect an error in this scenario, so the connector is expected " +
            "to catch that error and return a valid dataset with zero rows in it.");
    }

    @Test
    void invalidQuery() {
        DataFrameReader reader = newDefaultReader().option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'ViewNotFound')");
        RuntimeException ex = assertThrows(RuntimeException.class, () -> reader.load());

        assertTrue(ex.getMessage().startsWith("Unable to run Optic DSL query op.fromView('Medical', 'ViewNotFound'); cause: "),
            "If the query throws an error for any reason other than no rows being found, the error should be wrapped " +
                "in a new error with a message containing the user's query to assist with debugging; actual " +
                "message: " + ex.getMessage());
    }

    @Test
    void invalidCredentials() {
        DataFrameReader reader = newDefaultReader().option("spark.marklogic.client.password", "invalid password");
        RuntimeException ex = assertThrows(RuntimeException.class, () -> reader.load());

        assertEquals("Unable to connect to MarkLogic; status code: 401; error message: Unauthorized", ex.getMessage(),
            "The connector should verify that it can connect to MarkLogic before it tries to analyze the user's query. " +
                "This allows for a more helpful error message so that the user can focus on fixing their connection and " +
                "authentication properties.");
    }

    @Test
    void nonNumericPartitionCount() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, "abc")
            .load();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reader.count());
        assertEquals("Value of 'spark.marklogic.read.numPartitions' option must be numeric", ex.getMessage());
    }

    @Test
    void partitionCountLessThanOne() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, "0")
            .load();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reader.count());
        assertEquals("Value of 'spark.marklogic.read.numPartitions' option must be 1 or greater", ex.getMessage());
    }

    @Test
    void nonNumericBatchSize() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_BATCH_SIZE, "abc")
            .load();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reader.count());
        assertEquals("Value of 'spark.marklogic.read.batchSize' option must be numeric", ex.getMessage());
    }

    @Test
    void batchSizeLessThanZero() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_BATCH_SIZE, "-1")
            .load();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reader.count());
        assertEquals("Value of 'spark.marklogic.read.batchSize' option must be 0 or greater", ex.getMessage());
    }
}
