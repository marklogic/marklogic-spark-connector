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
        return newSparkSession()
            .read()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", testConfig.getUsername())
            .option("spark.marklogic.client.password", testConfig.getPassword())
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
