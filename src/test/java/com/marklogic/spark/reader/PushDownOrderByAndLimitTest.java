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

import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PushDownOrderByAndLimitTest extends AbstractPushDownTest {

    @Test
    void orderByWithOneBucket() {
        List<Row> rows = newDatasetOrderedByCitationIDWithOneBucket()
            // No need for a Spark orderBy here as there's a single bucket, so MarkLogic is able to take care of everything.
            .collectAsList();

        assertEquals(15, rows.size());
        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void orderByWithTwoPartitions() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_ORDERED_BY_CITATION_ID)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            // With 2+ partitions, the rows from each partition reader will be ordered correctly by MarkLogic, but
            // Spark needs to order the merged result. So Spark's orderBy must be called. Interestingly, since no
            // limit was set, Spark won't push down the orderBy call here. So the orderBy in the Optic query isn't
            // actually needed since Spark has to do the ordering regardless. Whether including the orderBy in
            // the Optic query makes the overall process faster or slower would need to be verified via testing for
            // any particular dataset and query.
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(15, rows.size());
        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void limitWithOnePartition() {
        long count = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 5)
            .load()
            .limit(1)
            .count();

        assertEquals(1, count);
        assertEquals(1, countOfRowsReadFromMarkLogic, "The batch size of 5 should cause 3 requests to be made to " +
            "MarkLogic, since there are 15 matching rows. With 1 partition, those 3 requests would be made by the " +
            "same partition reader. But since the first partition reader will most likely read at least 1 row, " +
            "Spark will stop calling the partition reader once it gets that 1 row in the first request to " +
            "MarkLogic. This is also due to the connector telling Spark that the limit is only 'partially' pushed " +
            "due to more than one bucket existing. That tells Spark that as soon as it has read back 1 row from the " +
            "reader, it should stop reading any more data, thereby ignoring the other 2 buckets. ");
    }

    @Test
    void limitWithTwoPartitions() {
        long count = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            .limit(1)
            .count();
        assertEquals(1, count);
        assertEquals(2, countOfRowsReadFromMarkLogic, "This test is intended to show that when 'limit' is used with " +
            "2 or more partitions, the limit will be applied to each partition reader. And that will result in more " +
            "rows than 'limit' being read from MarkLogic. Spark will still apply the 'limit' and only return 1 row " +
            "to the user, but the call won't be quite as efficient since more rows are being read than desired. Note " +
            "that there's a slightly chance this test can intermittently fail in the event that all 15 rows are " +
            "randomly assigned to the same partition.");
    }

    @Test
    void limitWithOnePartitionAndOrderBy() {
        List<Row> rows = newDatasetOrderedByCitationIDWithOneBucket()
            .limit(6)
            .collectAsList();

        assertEquals(6, rows.size());
        assertEquals(6, countOfRowsReadFromMarkLogic, "Because there's only one bucket, only 6 rows should be " +
            "returned from MarkLogic. And there's no need for the user to call Spark's orderBy, since the single " +
            "partition will returned ordered rows.");

        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void limitWithTwoPartitionsAndOrderBy() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            .orderBy("CitationID")
            .limit(6)
            .collectAsList();

        assertEquals(6, rows.size());
        assertTrue(countOfRowsReadFromMarkLogic > 6, "Expecting each partition to read back up to 6 ordered rows, " +
            "as both the limit and the orderBy should have been pushed down to MarkLogic. Spark is then expected to " +
            "merge the rows together and re-apply the order/limit so that the user gets the expected response. There " +
            "is a slight chance this assertion will fail if all 15 author rows are randomly assigned to the same " +
            "partition. Unexpected count: " + countOfRowsReadFromMarkLogic);

        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void limitWithTwoPartitionsAndOrderByLastNameDescending() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '')")
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            .orderBy(new Column("LastName").desc())
            .limit(3)
            .collectAsList();

        assertEquals(3, rows.size());
        assertTrue(countOfRowsReadFromMarkLogic > 3, "Because two partitions were used with a limit of 3, it's " +
            "expected that each partition reader will read at least 1 row (and up to 3 rows) from MarkLogic. There " +
            "is a slight chance this assertion will fail if all 15 author rows are randomly assigned to the same " +
            "partition. Unexpected count: " + countOfRowsReadFromMarkLogic);

        assertEquals("Wooles", rows.get(0).getAs("LastName"));
        assertEquals("Tonnesen", rows.get(1).getAs("LastName"));
        assertEquals("Shoebotham", rows.get(2).getAs("LastName"));
    }

    @Test
    void orderByWithSchemaAndView() {
        List<Row> rows = newDefaultReader()
            .load()
            .orderBy("`Medical.Authors.CitationID`")
            .limit(10)
            .collectAsList();

        assertEquals(10, rows.size());
        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void orderByWithView() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', 'myQualifier')")
            .load()
            .orderBy("`myQualifier.CitationID`")
            .limit(8)
            .collectAsList();

        assertEquals(8, rows.size());
        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void sort() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .sort("CitationID")
            .limit(8)
            .collectAsList();

        assertEquals(8, rows.size());
        verifyRowsAreOrderedByCitationID(rows);
    }

    @Test
    void sortByMultiple() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            // Force a single request to ensure the orderBy is constructed correctly.
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load()
            .sort("CitationID", "LastName")
            .limit(8)
            .collectAsList();

        assertEquals(8, rows.size());
        verifyRowsAreOrderedByCitationID(rows);

        // Verify the first few rows to make sure they're sorted by LastName as well based on known values.
        final String column = "LastName";
        assertEquals("Awton", rows.get(0).getAs(column));
        assertEquals("Bernadzki", rows.get(1).getAs(column));
        assertEquals("Canham", rows.get(2).getAs(column));
    }

    private void verifyRowsAreOrderedByCitationID(List<Row> rows) {
        // Lowest known CitationID is 1, so start comparisons against that.
        long previousValue = 1;
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            long citationId = row.getLong(0);
            assertTrue(citationId > previousValue || citationId == previousValue,
                "Out-of-order row: " + row + "; index: " + i + "; previous value: " + previousValue);
            previousValue = citationId;
        }
    }
}
