package com.marklogic.spark;

import com.marklogic.client.eval.ServerEvaluationCall;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Map;

public class CustomCodeContext extends ContextSupport {

    private final StructType schema;
    private final boolean customSchema;

    public CustomCodeContext(Map<String, String> properties, StructType schema) {
        super(properties);
        this.schema = schema;
        boolean isDefaultSchema = schema.fields().length == 1 &&
            "URI".equals(schema.fields()[0].name()) &&
            DataTypes.StringType.equals(schema.fields()[0].dataType());
        this.customSchema = !isDefaultSchema;
    }

    public StructType getSchema() {
        return schema;
    }

    public ServerEvaluationCall buildCall(String invokeOption, String javascriptOption, String xqueryOption) {
        ServerEvaluationCall call = connectToMarkLogic().newServerEval();
        final Map<String, String> properties = getProperties();
        if (optionExists(invokeOption)) {
            return call.modulePath(properties.get(invokeOption));
        } else if (optionExists(javascriptOption)) {
            return call.javascript(properties.get(javascriptOption));
        } else if (optionExists(xqueryOption)) {
            return call.xquery(properties.get(xqueryOption));
        }
        throw new RuntimeException("Must specify one of the following options: " + Arrays.asList(
            invokeOption, javascriptOption, xqueryOption
        ));
    }

    public boolean isCustomSchema() {
        return customSchema;
    }
}
