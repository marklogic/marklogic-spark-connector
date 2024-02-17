package com.marklogic.spark.reader.file;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public abstract class FileRowSchema {

    // Same as Spark's binaryType.
    // See https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html .
    public static final StructType SCHEMA = new StructType()
        .add("path", DataTypes.StringType, false)
        .add("modificationTime", DataTypes.TimestampType)
        .add("length", DataTypes.LongType)
        .add("content", DataTypes.BinaryType, false);

    private FileRowSchema() {
    }

}
