/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownParamTest extends AbstractPushDownTest {
    static final String BIND_QUERY = "op.fromView('Medical', 'Incidents', '')"
    + ".where(fn.contains($c.Diagnosis, op.param('searchTerm')))";

    /**
     * This test demonstrates a parameter binding on an optic query.
     * Bindings are useful for search term expansion, transforming or casting a value,
     * embedding in op.fromSPARQL accessors, and other advanced functionality.
     */
    @Test
    void equalToWithOpticParam() {
        assertEquals(1, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "searchTerm", "default_value")
            .load()
            .filter("searchTerm == 'heat-stroke'")
            .collectAsList()
            .size(), "Verifying that an optic binding can be parameterized");
    }

    /**
     * This test makes sure that the default value is used when a matching filter is not pushed down.
     */
    @Test
    void equalToWithDefaultParam() {
        assertEquals(1, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "searchTerm", "heat-stroke")
            .load()
            .collectAsList()
            .size(), "Verifying that a default parameter is applied");
    }
}