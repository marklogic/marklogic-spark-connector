/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public abstract class TextExtractor {

    // Per https://tika.apache.org/3.0.0/configuring.html , a user can configure Tika via the TIKA_CONFIG environment
    // variable. Thus, we don't need to provide any options for configuring this object.
    private static final Tika tika = new Tika();

    public static UserDefinedFunction build() {
        return functions.udf(TextExtractor::extractText, DataTypes.StringType);
    }

    private static String extractText(Object binaryContent) throws IOException, TikaException {
        if (!(binaryContent instanceof byte[])) {
            throw new IllegalArgumentException(
                "Text extraction UDF must be run against a column containing non-null byte arrays."
            );
        }

        try (ByteArrayInputStream stream = new ByteArrayInputStream((byte[]) binaryContent)) {
            // Once we move this out of a UDF, we can retuern the text and a Map<String, String>
            // Can add that to DocumentInputs.
            Metadata metadata = new Metadata();
            String result = tika.parseToString(stream, metadata);
            System.out.println("MD: " + metadata);
            return result;
        }
    }

    private TextExtractor() {
    }
}
