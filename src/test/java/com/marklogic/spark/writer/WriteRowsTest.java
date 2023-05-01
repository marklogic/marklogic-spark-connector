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

import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;


public class WriteRowsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        SparkSession sparkSession = newSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", true)
                .format("csv")
                .csv("src/test/ml-data/data.csv"); // Path from repository root

        // TODO: The dataset passed in should be available to read (TableProvider or DataWriter?)
        System.out.println(dataset); // [docNum: string, docName: string]
        StringBuffer headers = new StringBuffer();
        for(int i=0; i<dataset.schema().fields().length; i++) {
            headers.append(dataset.schema().fields()[i].name());
            headers.append(",");
        }
        System.out.println("************ Starting MarkLogicSparkWriteDriver ****************");
        DataFrameWriter<Row> dataFrameWriter = dataset
            .write()
            .format(MarkLogicWriteDataSource.class.getName())
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", testConfig.getUsername())
            .option("spark.marklogic.client.password", testConfig.getPassword())
            .option("spark.marklogic.client.authType", "digest")
            // TODO: The below schema passed in should be readable by the inferschema method.
            .option("schema", headers.toString())
            .mode(SaveMode.Append); // mode is needed, else the doc is not written

        dataFrameWriter.save();
        verifyDocIsWritten();
    }

    private void verifyDocIsWritten() {
        TextDocumentManager textDocumentManager = getDatabaseClient().newTextDocumentManager();
        assertNotNull(textDocumentManager.read("doc1.txt"));
    }
}
