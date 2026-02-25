/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.filter.IncrementalWriteConfig;
import com.marklogic.client.datamovement.filter.IncrementalWriteFilter;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test to verify the filter is built correctly based on different options. This ensures the connector is
 * correctly passing options to the Builder, and if it does that, we can assume based on the tests in the Java Client
 * that the Builder will produce a filter that behaves as expected.
 */
class BuildIncrementalWriteFilterTest {

    private Map<String, String> options = new HashMap<>();
    private IncrementalWriteFilter filter;

    @BeforeEach
    void setup() {
        options.put(Options.WRITE_INCREMENTAL, "true");
    }

    @Test
    void defaultFilter() {
        buildFilter();

        IncrementalWriteConfig config = filter.getConfig();
        assertEquals("incrementalWriteHash", config.getHashKeyName());
        assertNull(config.getTimestampKeyName());
        assertNotNull(config.getSkippedDocumentsConsumer(), "By default, the connector should configure a " +
            "consumer that can keep track of the count of skipped documents so that can be logged later.");
        assertTrue(config.isCanonicalizeJson());
        assertNull(config.getJsonExclusions());
        assertNull(config.getXmlExclusions());
        assertNull(config.getSchemaName());
        assertNull(config.getViewName());
    }

    @Test
    void everyOption() {
        options.put(Options.WRITE_INCREMENTAL_HASH_KEY_NAME, "customHash");
        options.put(Options.WRITE_INCREMENTAL_TIMESTAMP_KEY_NAME, "customTimestamp");
        options.put(Options.WRITE_INCREMENTAL_SCHEMA, "mySchema");
        options.put(Options.WRITE_INCREMENTAL_VIEW, "myView");
        options.put(Options.WRITE_INCREMENTAL_CANONICALIZE_JSON, "false");
        options.put(Options.WRITE_INCREMENTAL_JSON_EXCLUSIONS, "/field1\n/field2");
        options.put(Options.WRITE_INCREMENTAL_XML_EXCLUSIONS, "//field1\n/ns:parent/ns:field2");
        options.put(Options.XPATH_NAMESPACE_PREFIX + "ns", "http://example.com/ns1");
        buildFilter();

        IncrementalWriteConfig config = filter.getConfig();
        assertNotNull(config.getSkippedDocumentsConsumer());
        assertEquals("customHash", config.getHashKeyName());
        assertEquals("customTimestamp", config.getTimestampKeyName());
        assertEquals("mySchema", config.getSchemaName());
        assertEquals("myView", config.getViewName());
        assertFalse(config.isCanonicalizeJson());

        String[] jsonExclusions = config.getJsonExclusions();
        assertEquals(2, jsonExclusions.length);
        assertEquals("/field1", jsonExclusions[0]);
        assertEquals("/field2", jsonExclusions[1]);

        String[] xmlExclusions = config.getXmlExclusions();
        assertEquals(2, xmlExclusions.length);
        assertEquals("//field1", xmlExclusions[0]);
        assertEquals("/ns:parent/ns:field2", xmlExclusions[1]);

        Map<String, String> namespaces = config.getXmlNamespaces();
        assertEquals(1, namespaces.size());
        assertEquals("http://example.com/ns1", namespaces.get("ns"));
    }

    private void buildFilter() {
        StructType schemaDoesntMatterForThisTest = DocumentRowSchema.SCHEMA;
        WriteContext context = new WriteContext(schemaDoesntMatterForThisTest, options);
        filter = context.buildIncrementalWriteFilter();
    }
}
