/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GetSparkSessionTest {

    @Test
    void noSession() {
        ConnectorException ex = assertThrows(ConnectorException.class, Util::getSparkSession);
        assertEquals("Could not obtain a Spark session, no active or default one found.", ex.getMessage());
    }
}
