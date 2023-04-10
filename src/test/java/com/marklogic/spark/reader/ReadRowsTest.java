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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ReadRowsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        Dataset<Row> reader = newSparkSession().read()
            .schema(new StructType()
                .add("docNum", DataTypes.IntegerType)
                .add("docName", DataTypes.StringType))
            .format(MarkLogicReadDataSource.class.getName())
            .option("marklogic.client.host", testConfig.getHost())
            .option("marklogic.client.port", testConfig.getRestPort())
            .option("marklogic.client.username", testConfig.getUsername())
            .option("marklogic.client.password", testConfig.getPassword())
            .option("marklogic.client.authType", "digest")
            .option("marklogic.opticDsl", "op.fromView('Medical','Authors');")
            .load();

        List<Row> rows = reader.collectAsList();
        assertEquals(15, rows.size(),
            "Expecting all 15 rows to be returned from the view");
    }
}
