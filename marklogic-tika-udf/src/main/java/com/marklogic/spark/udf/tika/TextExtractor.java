/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf.tika;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.tika.Tika;

import java.io.ByteArrayInputStream;

/**
 * Maybe we build this in Flux instead? As a subproject?
 */
public abstract class TextExtractor {

    private static final Tika tika = new Tika();

    public static UserDefinedFunction build() {
        return functions.udf(
            binaryContent -> tika.parseToString(new ByteArrayInputStream((byte[]) binaryContent)),
            DataTypes.StringType
        ).asNonNullable();
    }

    private TextExtractor() {
    }
}
