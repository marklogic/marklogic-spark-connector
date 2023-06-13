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
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractPushDownTest extends AbstractIntegrationTest {

    final static String QUERY_WITH_NO_QUALIFIER = "op.fromView('Medical', 'Authors', '')";
    final static String QUERY_ORDERED_BY_CITATION_ID = "op.fromView('Medical', 'Authors', '').orderBy(op.col('CitationID'))";

    long countOfRowsReadFromMarkLogic;

    @BeforeEach
    void setup() {
        // This is used to track how many rows were read from MarkLogic. It's used to ensure that the filtering is
        // correctly pushed down to MarkLogic as opposed to being handled by Spark, which should make operations
        // faster as MarkLogic is returning fewer rows. A synchronized method is used in case the test uses multiple
        // partitions, as each will run on a separate thread.
        MarkLogicPartitionReader.totalRowCountListener = totalRowCount -> addToRowCount(totalRowCount);
    }

    protected DataFrameReader newDefaultReader(SparkSession session) {
        return super.newDefaultReader(session)
            // Default to a single call to MarkLogic for push down tests to ensure that assertions on row counts are
            // accurate (and via DEVEXP-488, the batch size is expected to be set to zero when an aggregate is pushed
            // down). Any tests that care about having more than one partition are expected to override this.
            .option(Options.READ_NUM_PARTITIONS, 1);
    }

    private synchronized void addToRowCount(long totalRowCount) {
        countOfRowsReadFromMarkLogic += totalRowCount;
    }

    protected Dataset<Row> newDatasetOrderedByCitationIDWithOneBucket() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_ORDERED_BY_CITATION_ID)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();
    }

}
