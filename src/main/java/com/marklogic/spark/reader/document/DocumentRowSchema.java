package com.marklogic.spark.reader.document;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public abstract class DocumentRowSchema {

    public static final StructType SCHEMA = new StructType()
        .add("URI", DataTypes.StringType)
        .add("content", DataTypes.BinaryType)
        .add("format", DataTypes.StringType);

    private DocumentRowSchema() {
    }
}
