package com.marklogic.spark;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.spark.reader.MarkLogicTableProvider;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Uses marklogic-junit (from marklogic-unit-test) to construct a DatabaseClient
 * based on the properties in gradle.properties and gradle-local.properties.
 * <p>
 * Use this as the base class for all tests that need to connect to MarkLogic.
 */
public class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    @Autowired
    protected SimpleTestConfig testConfig;

    @Override
    public void deleteDocumentsBeforeTestRuns() {
        // Nothing to delete yet
    }

    protected SparkSession newSparkSession() {
        return SparkSession.builder()
            .master("local[*]")
            .getOrCreate();
    }

    /**
     * For tests that need a default config, at which point they'll make any other calls they need to
     * load a dataset.
     *
     * @return
     */
    protected DataFrameReader newDefaultReader() {
        return newSparkSession().read()
            .format(MarkLogicTableProvider.class.getName())
            .option("marklogic.client.host", testConfig.getHost())
            .option("marklogic.client.port", testConfig.getRestPort())
            .option("marklogic.client.username", testConfig.getUsername())
            .option("marklogic.client.password", testConfig.getPassword())
            .option("marklogic.client.authType", "digest")
            .option("marklogic.optic_dsl", "op.fromView('Medical','Authors');")
            .schema(new StructType()
                .add("Medical.Authors.CitationID", DataTypes.IntegerType)
                .add("Medical.Authors.LastName", DataTypes.StringType)
            );
    }
}
