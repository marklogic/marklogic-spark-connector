package com.marklogic.spark.reader.customcode;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
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
                properties::get
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
        } else if (optionExists(callOptions.javascriptFileOptionName)) {
            String content = readFileToString(properties.get(callOptions.javascriptFileOptionName));
            call.javascript(content);
        } else if (optionExists(callOptions.xqueryFileOptionName)) {
            String content = readFileToString(properties.get(callOptions.xqueryFileOptionName));
            call.xquery(content);
        } else {
            // The ETL tool validates this itself via a validator.
            throw new ConnectorException("Must specify one of the following options: " + Arrays.asList(
                callOptions.invokeOptionName, callOptions.javascriptOptionName, callOptions.xqueryOptionName
            ));
        }

        this.userDefinedVariables.forEach(call::addVariable);
        return call;
    }

    private String readFileToString(String file) {
        try {
            // commons-io is a Spark dependency, so safe to use.
            return FileUtils.readFileToString(new File(file), Charset.defaultCharset());
        } catch (IOException e) {
            String message = e.getMessage();
            if (e instanceof NoSuchFileException) {
                message += " was not found.";
            }
            throw new ConnectorException(String.format("Cannot read from file %s; cause: %s", file, message), e);
        }
    }

    public boolean isCustomSchema() {
        return customSchema;
    }

    boolean hasPartitionCode() {
        return hasOption(
            Options.READ_PARTITIONS_INVOKE,
            Options.READ_PARTITIONS_JAVASCRIPT,
            Options.READ_PARTITIONS_JAVASCRIPT_FILE,
            Options.READ_PARTITIONS_XQUERY,
            Options.READ_PARTITIONS_XQUERY_FILE
        );
    }

    // Intended solely to simplify passing these 3 option names around.
    public static class CallOptions {
        private final String invokeOptionName;
        private final String javascriptOptionName;
        private final String xqueryOptionName;
        private final String javascriptFileOptionName;
        private final String xqueryFileOptionName;

        public CallOptions(String invokeOptionName, String javascriptOptionName, String xqueryOptionName,
                           String javascriptFileOptionName, String xqueryFileOptionName) {
            this.javascriptOptionName = javascriptOptionName;
            this.xqueryOptionName = xqueryOptionName;
            this.invokeOptionName = invokeOptionName;
            this.javascriptFileOptionName = javascriptFileOptionName;
            this.xqueryFileOptionName = xqueryFileOptionName;
        }
    }
}
