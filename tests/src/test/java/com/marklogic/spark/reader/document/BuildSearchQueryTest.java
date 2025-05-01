/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BuildSearchQueryTest extends AbstractIntegrationTest {

    @Test
    void transformWithDefaultDelimiter() {
        SearchQueryDefinition query = new SearchQueryBuilder()
            .withTransformName("myTransform")
            .withTransformParams("param1,value1,param2,value2")
            .buildQuery(getDatabaseClient());

        ServerTransform transform = query.getResponseTransform();
        assertEquals("myTransform", transform.getName());
        assertEquals(2, transform.size());
        assertEquals("value1", transform.get("param1").get(0));
        assertEquals("value2", transform.get("param2").get(0));
    }

    @Test
    void transformWithCustomDelimiter() {
        SearchQueryDefinition query = new SearchQueryBuilder()
            .withTransformName("myTransform")
            .withTransformParams("p1!v1!p2!v2")
            .withTransformParamsDelimiter("!")
            .buildQuery(getDatabaseClient());

        ServerTransform transform = query.getResponseTransform();
        assertEquals("myTransform", transform.getName());
        assertEquals(2, transform.size());
        assertEquals("v1", transform.get("p1").get(0));
        assertEquals("v2", transform.get("p2").get(0));
    }

    @Test
    void wrongNumberOfParams() {
        DatabaseClient client = getDatabaseClient();
        SearchQueryBuilder builder = new SearchQueryBuilder()
            .withTransformName("myTransform")
            .withTransformParams("param1,value1,param2");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> builder.buildQuery(client));
        assertEquals("Transform parameters must have an equal number of parameter names and values: param1,value1,param2", ex.getMessage());
    }
}
