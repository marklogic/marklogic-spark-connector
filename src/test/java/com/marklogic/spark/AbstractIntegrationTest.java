package com.marklogic.spark;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.spark.reader.ReadConstants;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;

/**
 * Uses marklogic-junit (from marklogic-unit-test) to construct a DatabaseClient
 * based on the properties in gradle.properties and gradle-local.properties.
 * <p>
 * Use this as the base class for all tests that need to connect to MarkLogic.
 */
public class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    // User credentials for all calls to MarkLogic by the Spark connector
    protected final static String TEST_USERNAME = "spark-test-user";
    protected final static String TEST_PASSWORD = "spark";

    /**
     * Via marklogic-junit5, this is populated via the mlHost/mlRestPort/mlUsername/mlPassword property values. Those
     * are expected to be for an admin-like user who can deploy the test app. Thus, this should only be used for
     * operations requiring an admin-like user.
     */
    @Autowired
    protected SimpleTestConfig testConfig;

    @Override
    public void deleteDocumentsBeforeTestRuns() {
        // Nothing to delete yet
    }

    protected SparkSession newSparkSession() {
        return newSparkSession("UTC");
    }

    protected SparkSession newSparkSession(String timeZone) {
        return SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.session.timeZone", timeZone)
            .getOrCreate();
    }

    /**
     * For tests that need a default config, at which point they'll make any other calls they need to
     * load a dataset.
     *
     * @return
     */
    protected DataFrameReader newDefaultReader() {
        return newDefaultReader(newSparkSession());
    }

    protected DataFrameReader newDefaultReader(SparkSession session) {
        return session
            .read()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", TEST_USERNAME)
            .option("spark.marklogic.client.password", TEST_PASSWORD)
            .option("spark.marklogic.client.authType", "digest")
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical','Authors')");
    }

    protected String readClasspathFile(String path) {
        try {
            return new String(FileCopyUtils.copyToByteArray(new ClassPathResource(path).getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
