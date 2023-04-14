package com.marklogic.spark.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
            .format(MarkLogicTableProvider.class.getName())
            .option("marklogic.client.host", "localhost")
            .option("marklogic.client.port", 8009)
            .option("marklogic.client.username", "admin")
            .option("marklogic.client.password", "admin")
            .option("marklogic.client.authType", "digest")
            .option("marklogic.optic_dsl", "op.fromView('demo','employee')")
            .option("marklogic.num_partitions", partitionCount)
            .option("marklogic.batch_size", batchSize)
            .schema(new StructType()
                .add("employee_id", DataTypes.IntegerType)
                .add("person_id", DataTypes.IntegerType)
                .add("job_description", DataTypes.StringType)
            )
            .load();

        logger.info("Collecting rows as list");
        long now = System.currentTimeMillis();
//        List<Row> rows = dataset.collectAsList();
        long count = dataset.count();
        logger.info("Duration: " + (System.currentTimeMillis() - now));
        logger.info("COUNT: " + count);
//        rows.forEach(row -> logger.info(row.prettyJson()));
    }
}
