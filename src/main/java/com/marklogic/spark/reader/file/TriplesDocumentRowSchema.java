package com.marklogic.spark.reader.file;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The idea with this is that we build the document as we read triples, thereby allowing us to include the
 * undocumented sem:origin element.
 */
public abstract class TriplesDocumentRowSchema {

    public static final StructType SCHEMA = new StructType()
        .add("URI", DataTypes.StringType)
        .add("content", DataTypes.StringType)
        .add("graph", DataTypes.StringType);
}
