package com.marklogic.spark;

import org.apache.spark.sql.SparkSession;

/**
 * This is only intended for manual testing with spark-submit. As noted in the CONTRIBUTING.md file, you first need to
 * move this to src/main/java and then rebuild the connector jar.
 */
public class TestProgram {

    public static void main(String[] args) {
        SparkSession.builder().getOrCreate().read()
            .format("com.marklogic.spark")
            .option(Options.CLIENT_URI, "spark-test-user:spark@localhost:8016")
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors')")
            .load()
            .collectAsList()
            .forEach(row -> System.out.println("ROW: " + row.prettyJson()));
    }
}
