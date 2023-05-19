package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

    private synchronized void addToRowCount(long totalRowCount) {
        countOfRowsReadFromMarkLogic += totalRowCount;
    }

    protected Dataset<Row> newDatasetOrderedByCitationIDWithOneBucket() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_DSL, QUERY_ORDERED_BY_CITATION_ID)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();
    }

}
