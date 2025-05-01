/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import org.apache.spark.SparkException;
import org.apache.spark.sql.*;
import org.apache.spark.util.VersionUtils;
import org.jdom2.Namespace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Uses marklogic-junit (from marklogic-unit-test) to construct a DatabaseClient
 * based on the properties in gradle.properties and gradle-local.properties.
 * <p>
 * Use this as the base class for all tests that need to connect to MarkLogic.
 */
@ContextConfiguration(classes = {TestConfig.class})
@ExtendWith(IOExceptionTestExecutionListener.class)
public abstract class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    // User credentials for all calls to MarkLogic by the Spark connector
    protected static final String TEST_USERNAME = "spark-test-user";
    protected static final String TEST_PASSWORD = "spark";
    protected static final String CONNECTOR_IDENTIFIER = "marklogic";
    protected static final String NO_AUTHORS_QUERY = "op.fromView('Medical', 'NoAuthors', '')";
    protected static final String DEFAULT_PERMISSIONS = "spark-user-role,read,spark-user-role,update";
    protected static final Namespace PROPERTIES_NAMESPACE = Namespace.getNamespace("prop", "http://marklogic.com/xdmp/property");

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Via marklogic-junit5, this is populated via the mlHost/mlRestPort/mlUsername/mlPassword property values. Those
     * are expected to be for an admin-like user who can deploy the test app. Thus, this should only be used for
     * operations requiring an admin-like user.
     */
    @Autowired
    protected TestConfig testConfig;

    protected SparkSession sparkSession;

    @BeforeEach
    void logConnectionString() {
        logger.info("Default connection string: {}", makeClientUri());
    }

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
            .option(Options.CLIENT_HOST, testConfig.getHost())
            .option(Options.CLIENT_PORT, testConfig.getRestPort())
            .option(Options.CLIENT_USERNAME, TEST_USERNAME)
            .option(Options.CLIENT_PASSWORD, TEST_PASSWORD)
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

    protected final boolean isSpark340OrHigher() {
        assertNotNull(sparkSession, "Cannot check Spark version until a Spark Session has been created.");
        final String version = sparkSession.version();
        int major = VersionUtils.majorVersion(version);
        int minor = VersionUtils.minorVersion(version);
        return major > 3 || (major == 3 && minor >= 4);
    }

    protected final String rowsToString(List<Row> rows) {
        // Used for debugging and in some assertions.
        return rows.stream().map(Row::prettyJson).collect(Collectors.joining());
    }

    /**
     * Avoids having to repeat mode/save.
     */
    protected void defaultWrite(DataFrameWriter writer) {
        writer.options(defaultWriteOptions())
            .mode(SaveMode.Append)
            .save();
    }

    /**
     * Nearly every successful write operation will want these options.
     */
    protected Map<String, String> defaultWriteOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.CLIENT_URI, makeClientUri());
        options.put(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS);
        return options;
    }

    protected final ConnectorException assertThrowsConnectorException(Runnable r) {
        SparkException ex = assertThrows(SparkException.class, r::run);
        if (ex.getCause() instanceof SparkException) {
            ex = (SparkException) ex.getCause();
        }
        assertTrue(ex.getCause() instanceof ConnectorException,
            "Expect the Spark-thrown SparkException to wrap our ConnectorException, which is an exception that we " +
                "intentionally throw when an error condition is detected. " +
                "Actual exception cause type: " + ex.getCause());
        return (ConnectorException) ex.getCause();
    }

    protected final DocumentMetadataHandle readMetadata(String uri) {
        // This should really be in marklogic-unit-test.
        return getDatabaseClient().newDocumentManager().readMetadata(uri, new DocumentMetadataHandle());
    }

    @Override
    protected XmlNode readDocumentProperties(String uri) {
        // This should be fixed in marklogic-unit-test to include the properties namespace by default.
        XmlNode props = super.readDocumentProperties(uri);
        props.setNamespaces(new Namespace[]{PROPERTIES_NAMESPACE});
        return props;
    }

    @Override
    protected XmlNode readXmlDocument(String uri) {
        // Registers frequently used namespaces in tests.
        return readXmlDocument(uri,
            Namespace.getNamespace("model", "http://marklogic.com/appservices/model"),
            Namespace.getNamespace("ex", "org:example"),
            Namespace.getNamespace("acme", "org:acme"),

            // These are specific to text extraction tests, but no harm in including them here.
            Namespace.getNamespace("pdf", "http://ns.adobe.com/pdf/1.3/"),
            Namespace.getNamespace("dc", "http://purl.org/dc/elements/1.1/")
        );
    }


}
