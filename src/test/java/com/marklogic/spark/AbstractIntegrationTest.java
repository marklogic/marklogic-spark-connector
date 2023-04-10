package com.marklogic.spark;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import org.apache.spark.sql.SparkSession;
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
}
