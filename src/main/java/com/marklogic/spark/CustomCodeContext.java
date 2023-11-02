package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
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

    public ServerEvaluationCall buildCall(DatabaseClient client, CallInputs callInputs) {
        ServerEvaluationCall call = client.newServerEval();
        final Map<String, String> properties = getProperties();
        if (optionExists(callInputs.invokeOptionName)) {
            call.modulePath(properties.get(callInputs.invokeOptionName));
        } else if (optionExists(callInputs.javascriptOptionName)) {
            call.javascript(properties.get(callInputs.javascriptOptionName));
        } else if (optionExists(callInputs.xqueryOptionName)) {
            call.xquery(properties.get(callInputs.xqueryOptionName));
        } else {
            throw new RuntimeException("Must specify one of the following options: " + Arrays.asList(
                callInputs.invokeOptionName, callInputs.javascriptOptionName, callInputs.xqueryOptionName
            ));
        }

        addUserDefinedVariables(call, callInputs);
        return call;
    }

    private void addUserDefinedVariables(ServerEvaluationCall call, CallInputs callInputs) {
        final Map<String, String> properties = getProperties();
        properties.keySet().forEach(propertyName -> {
            if (propertyName.startsWith(callInputs.varsPrefix)) {
                String varName = propertyName.substring(callInputs.varsPrefix.length());
                call.addVariable(varName, properties.get(propertyName));
            }
        });
    }

    public boolean isCustomSchema() {
        return customSchema;
    }

    public static class CallInputs {
        private final String invokeOptionName;
        private final String javascriptOptionName;
        private final String xqueryOptionName;
        private final String varsPrefix;

        public CallInputs(String invokeOptionName, String javascriptOptionName, String xqueryOptionName, String varsPrefix) {
            this.javascriptOptionName = javascriptOptionName;
            this.xqueryOptionName = xqueryOptionName;
            this.invokeOptionName = invokeOptionName;
            this.varsPrefix = varsPrefix;
        }
    }
}
