package com.marklogic.spark.reader;

import com.marklogic.spark.DefaultSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not a test, but a handy way to run ad hoc performance tests against the "employee" documents created by the default
 * configuration in the quick-table project.
 * <p>
 * Some performance notes:
 * <p>
 * For 100k rows with Spark task count of 16, showing partitions;duration:
 * <p>
 * 1;65.9
 * 4;15.3
 * 8;12.2
 * 10;12.5
 * 16;13.5
 * 24;14.1
 */
public class PerformanceTester {

    private final static Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

    public static void main(String[] args) {
        final int sparkConcurrentTaskCount = 16;
        final int partitionCount = 8;
        final int batchSize = 10000;

        Dataset<Row> dataset = SparkSession.builder()
            .master(String.format("local[%d]", sparkConcurrentTaskCount))
            .getOrCreate()
            .read()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", "localhost")
            .option("spark.marklogic.client.port", 8009)
            .option("spark.marklogic.client.username", "admin")
            .option("spark.marklogic.client.password", "admin")
            .option("spark.marklogic.client.authType", "digest")
            .option(ReadConstants.OPTIC_DSL, "op.fromView('demo','employee')")
            .option(ReadConstants.NUM_PARTITIONS, partitionCount)
            .option(ReadConstants.BATCH_SIZE, batchSize)
            .load();

        long now = System.currentTimeMillis();
        long count = dataset.count();
        logger.info("Duration: " + (System.currentTimeMillis() - now));
        logger.info("COUNT: " + count);
//        rows.forEach(row -> logger.info(row.prettyJson()));
    }
}
