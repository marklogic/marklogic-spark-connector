/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Represents a triple as read from an RDF file and serialized into the 3 XML elements comprising
 * a MarkLogic triple.
 */
public abstract class TripleRowSchema {

    public static final StructType SCHEMA = new StructType()
        .add("subject", DataTypes.StringType)
        .add("predicate", DataTypes.StringType)
        .add("object", DataTypes.StringType)
        .add("datatype", DataTypes.StringType)
        .add("lang", DataTypes.StringType)
        .add("graph", DataTypes.StringType);

    private TripleRowSchema() {
    }
}
