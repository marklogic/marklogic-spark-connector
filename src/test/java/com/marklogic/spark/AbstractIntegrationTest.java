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
package com.marklogic.spark;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.VersionUtils;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Uses marklogic-junit (from marklogic-unit-test) to construct a DatabaseClient
 * based on the properties in gradle.properties and gradle-local.properties.
 * <p>
 * Use this as the base class for all tests that need to connect to MarkLogic.
 */
public abstract class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    // User credentials for all calls to MarkLogic by the Spark connector
    protected static final String TEST_USERNAME = "spark-test-user";
    protected static final String TEST_PASSWORD = "spark";
    protected static final String CONNECTOR_IDENTIFIER = "marklogic";
    protected static final String NO_AUTHORS_QUERY = "op.fromView('Medical', 'NoAuthors', '')";

    private static MarkLogicVersion markLogicVersion;

    /**
     * Via marklogic-junit5, this is populated via the mlHost/mlRestPort/mlUsername/mlPassword property values. Those
     * are expected to be for an admin-like user who can deploy the test app. Thus, this should only be used for
     * operations requiring an admin-like user.
     */
    @Autowired
    protected SimpleTestConfig testConfig;

    protected SparkSession sparkSession;

    @AfterEach
    public void closeSparkSession() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }

    @Override
    protected String getJavascriptForDeletingDocumentsBeforeTestRuns() {
        return "declareUpdate(); " +
            "for (var uri of cts.uris(null, null, cts.notQuery(cts.collectionQuery('test-config')))) {" +
            "  xdmp.documentDelete(uri);" +
            "}";
    }

    protected SparkSession newSparkSession() {
        return newSparkSession("UTC");
    }

    protected SparkSession newSparkSession(String timeZone) {
        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.session.timeZone", timeZone)
            .getOrCreate();
        return sparkSession;
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
            .format(CONNECTOR_IDENTIFIER)
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", TEST_USERNAME)
            .option("spark.marklogic.client.password", TEST_PASSWORD)
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors')");
    }

    protected String readClasspathFile(String path) {
        try {
            return new String(FileCopyUtils.copyToByteArray(new ClassPathResource(path).getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected final String makeClientUri() {
        return String.format("%s:%s@%s:%d", TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort());
    }

    protected final boolean isMarkLogic10() {
        if (markLogicVersion == null) {
            String version = getDatabaseClient().newServerEval().javascript("xdmp.version()").evalAs(String.class);
            markLogicVersion = new MarkLogicVersion(version);
        }
        return markLogicVersion.getMajor() == 10;
    }

    protected final boolean isSpark340OrHigher() {
        assertNotNull(sparkSession, "Cannot check Spark version until a Spark Session has been created.");
        final String version = sparkSession.version();
        int major = VersionUtils.majorVersion(version);
        int minor = VersionUtils.minorVersion(version);
        return major > 3 || (major == 3 && minor >= 4);
    }

    protected final String rowsToString(List<Row> rows) {
        // Used for debugging and in some assertions.
        return rows.stream().map(row -> row.prettyJson()).collect(Collectors.joining());
    }
}
