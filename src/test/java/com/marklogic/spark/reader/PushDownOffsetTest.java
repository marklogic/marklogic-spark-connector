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
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushDownOffsetTest extends AbstractPushDownTest {

    @Test
    void offset() {
        List<Row> rows = newDatasetOrderedByCitationIDWithOneBucket()
            .filter("CitationID > 1")
            .offset(4)
            .collectAsList();

        assertEquals(7, rows.size(),
            "Expecting the 4 authors with CitationID=3; then the 1 with CitationID=4; then the 2 with CitationID=5");
        assertEquals(7, countOfRowsReadFromMarkLogic);
    }

    @Test
    void noRowsFound() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, NO_AUTHORS_QUERY)
            .load()
            .offset(1)
            .collectAsList();

        assertEquals(0, rows.size());
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void limitBeforeOffset() {
        List<Row> rows = newDatasetOrderedByCitationIDWithOneBucket()
            // Filter has to be before offset, otherwise Spark doesn't push it down. Spark will still apply the filter,
            // but the connector never hears about it.
            .filter("CitationID > 1")
            .limit(8)
            .offset(4)
            .collectAsList();

        verifyTheFourAuthorsWithCitationIDOfThreeWereReturned(rows);
    }

    @Test
    void offsetBeforeLimit() {
        List<Row> rows = newDatasetOrderedByCitationIDWithOneBucket()
            .filter("CitationID > 1")
            // Setting offset before limit seems like the more natural way to express the goal of "I want rows 5
            // through 8" - i.e. skip the first 4 rows, and then give me the next 4. Under the hood, Spark actually
            // passes a limit of 8 to the connector though, since Spark thinks in terms of "I'll first express
            // the total number of rows to be retrieved, and then the offset within that range, and I should only get
            // those rows".
            .offset(4)
            .limit(4)
            .collectAsList();

        verifyTheFourAuthorsWithCitationIDOfThreeWereReturned(rows);
    }

    private void verifyTheFourAuthorsWithCitationIDOfThreeWereReturned(List<Row> rows) {
        assertEquals(4, rows.size());
        rows.forEach(row -> assertEquals(3, (long) row.getAs("CitationID"),
            "Expecting to get back the 4 authors with CitationID=3, as the 4 authors with CitationID=2 should have " +
                "been skipped via the offset."));
        assertEquals(4, countOfRowsReadFromMarkLogic, "Because there's a single bucket, the connector should tell " +
            "Spark that the pushdown was 'full' and not partial, which means the connector only returns 4 rows and " +
            "Spark doesn't need to apply the limit itself");
    }
}
