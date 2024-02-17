package com.marklogic.spark.reader.document;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public abstract class DocumentRowSchema {

    public static final StructType SCHEMA = new StructType()
        .add("URI", DataTypes.StringType, false)
        .add("content", DataTypes.BinaryType)
        .add("format", DataTypes.StringType)
        .add("collections", DataTypes.createArrayType(DataTypes.StringType))
        .add("permissions", DataTypes.createMapType(
            DataTypes.StringType,
            DataTypes.createArrayType(DataTypes.StringType))
        )
        .add("quality", DataTypes.IntegerType)
        .add("properties", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
        .add("metadataValues", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

    private DocumentRowSchema() {
    }
}
