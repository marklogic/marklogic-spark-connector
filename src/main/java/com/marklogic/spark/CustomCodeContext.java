package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class CustomCodeContext extends ContextSupport {

    private final StructType schema;
    private final boolean customSchema;
    private final Map<String, String> userDefinedVariables;

    public CustomCodeContext(Map<String, String> properties, StructType schema, String userDefinedVariablesPrefix) {
        super(properties);
        this.schema = schema;
        boolean isDefaultSchema = schema.fields().length == 1 &&
            DataTypes.StringType.equals(schema.fields()[0].dataType());
        this.customSchema = !isDefaultSchema;

        // Stash a map of the user-defined variables (if any) so that they aren't calculated on every call.
        this.userDefinedVariables = properties.keySet().stream()
            .filter(key -> key.startsWith(userDefinedVariablesPrefix))
            .collect(Collectors.toMap(
                key -> key.substring(userDefinedVariablesPrefix.length()),
                key -> properties.get(key)
            ));
    }

    public StructType getSchema() {
        return schema;
    }

    public ServerEvaluationCall buildCall(DatabaseClient client, CallOptions callOptions) {
        ServerEvaluationCall call = client.newServerEval();
        final Map<String, String> properties = getProperties();
        if (optionExists(callOptions.invokeOptionName)) {
            call.modulePath(properties.get(callOptions.invokeOptionName));
        } else if (optionExists(callOptions.javascriptOptionName)) {
            call.javascript(properties.get(callOptions.javascriptOptionName));
        } else if (optionExists(callOptions.xqueryOptionName)) {
            call.xquery(properties.get(callOptions.xqueryOptionName));
        } else {
            throw new RuntimeException("Must specify one of the following options: " + Arrays.asList(
                callOptions.invokeOptionName, callOptions.javascriptOptionName, callOptions.xqueryOptionName
            ));
        }

        this.userDefinedVariables.forEach((key, value) -> call.addVariable(key, value));
        return call;
    }

    public boolean isCustomSchema() {
        return customSchema;
    }

    // Intended solely to simplify passing these 3 option names around.
    public static class CallOptions {
        private final String invokeOptionName;
        private final String javascriptOptionName;
        private final String xqueryOptionName;

        public CallOptions(String invokeOptionName, String javascriptOptionName, String xqueryOptionName) {
            this.javascriptOptionName = javascriptOptionName;
            this.xqueryOptionName = xqueryOptionName;
            this.invokeOptionName = invokeOptionName;
        }
    }
}
