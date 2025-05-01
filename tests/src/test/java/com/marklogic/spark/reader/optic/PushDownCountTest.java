/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownCountTest extends AbstractPushDownTest {

    @Test
    void count() {
        long count = newDefaultReader()
            .load()
            .count();

        assertEquals(15, count, "Expecting all 15 authors to be counted");
        assertRowsReadFromMarkLogic(1);
    }

    @Test
    void noRowsFound() {
        long count = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .count();

        assertEquals(0, count);
        assertRowsReadFromMarkLogic(0, "When no rows exist, neither the count() operation nor the " +
            "pruneColumns() operation should be pushed down since there's no optimization to be done.");
    }
}
