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
package com.marklogic.spark.writer;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

abstract class AbstractWriteTest extends AbstractIntegrationTest {

    protected final static String COLLECTION = "write-test";

    protected DataFrameWriter newWriter() {
        return newWriter(1);
    }

    protected DataFrameWriter newWriterForSingleRow() {
        return newWriterWithDefaultConfig("temporal-data.csv", 1);
    }

    protected DataFrameWriter newWriter(int partitionCount) {
        return newWriterWithDefaultConfig("data.csv", partitionCount);
    }

    protected DataFrameWriter newWriterWithDefaultConfig(String csvFilename, int partitionCount) {
        return newWriterWithoutDocumentConfig(csvFilename, partitionCount)
            .option(Options.WRITE_COLLECTIONS, COLLECTION)
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
            .option(Options.WRITE_URI_PREFIX, "/test/")
            .option(Options.WRITE_URI_SUFFIX, ".json");
    }

    protected DataFrameWriter newWriterWithoutDocumentConfig(String csvFilename, int partitionCount) {
        return newSparkSession().read()
            .option("header", true)
            .format("csv")
            .csv("src/test/resources/" + csvFilename)
            .repartition(partitionCount)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", "spark-test-user")
            .option("spark.marklogic.client.password", "spark");
    }

    /**
     * In Spark 3.3, an error from our writer - such as an IllegalArgumentException for an invalid option - is two
     * layers deep in a SparkException. In Spark 3.4, it's one layer deep. This method is used to hide that from
     * test classes so that when we upgrade from Spark 3.3 to Spark 3.4, it's easy to update the tests.
     *
     * @param ex
     * @return
     */
    protected final Throwable getCauseFromWriterException(Exception ex) {
        return isSpark340OrHigher() ? ex.getCause() : ex.getCause().getCause();
    }
}
