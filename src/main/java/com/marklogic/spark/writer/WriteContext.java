package com.marklogic.spark.writer;

import com.marklogic.spark.ContextSupport;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class WriteContext extends ContextSupport {

    final static long serialVersionUID = 1;
    
    private final StructType schema;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;
    }

    public StructType getSchema() {
        return schema;
    }
}
