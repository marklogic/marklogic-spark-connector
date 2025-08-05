/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

/**
 * Allows the logic for reading Jena quads and triples as Spark rows to be easily reused without being tied to a
 * specific Spark partition reader.
 */
public interface RdfStreamReader {

    boolean hasNext() throws IOException;

    InternalRow get();
}
