/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomCodeCallBuilder {

    private final Map<String, String> userDefinedVariables;
    private final Function<DatabaseClient, ServerEvaluationCall> callBuilder;

    public static Optional<CustomCodeCallBuilder> build(Context context, CallOptions callOptions, String varsPrefix, boolean required) {
        CustomCodeCallBuilder builder = new CustomCodeCallBuilder(context, callOptions, varsPrefix);
        if (required && builder.callBuilder == null) {
            // Flux validates this itself via a validator.
            throw new ConnectorException("Must specify one of the following options: " + Arrays.asList(
                callOptions.invokeOptionName, callOptions.javascriptOptionName, callOptions.xqueryOptionName,
                callOptions.javascriptFileOptionName, callOptions.xqueryFileOptionName
            ));
        }
        return builder.callBuilder != null ? Optional.of(builder) : Optional.empty();
    }

    private CustomCodeCallBuilder(Context context, CallOptions callOptions, String varsPrefix) {
        this.callBuilder = initializeCallBuilder(context, callOptions);

        if (varsPrefix != null) {
            final Map<String, String> properties = context.getProperties();
            this.userDefinedVariables = properties.keySet().stream()
                .filter(key -> key.startsWith(varsPrefix))
                .collect(Collectors.toMap(
                    key -> key.substring(varsPrefix.length()),
                    properties::get
                ));
        } else {
            this.userDefinedVariables = Map.of();
        }
    }

    private Function<DatabaseClient, ServerEvaluationCall> initializeCallBuilder(Context context, CallOptions callOptions) {
        final Map<String, String> properties = context.getProperties();
        if (context.optionExists(callOptions.invokeOptionName)) {
            String path = properties.get(callOptions.invokeOptionName);
            return client -> client.newServerEval().modulePath(path);
        } else if (context.optionExists(callOptions.javascriptOptionName)) {
            String javascript = properties.get(callOptions.javascriptOptionName);
            return client -> client.newServerEval().javascript(javascript);
        } else if (context.optionExists(callOptions.xqueryOptionName)) {
            String xquery = properties.get(callOptions.xqueryOptionName);
            return client -> client.newServerEval().xquery(xquery);
        } else if (context.optionExists(callOptions.javascriptFileOptionName)) {
            String content = readFileToString(properties.get(callOptions.javascriptFileOptionName));
            return client -> client.newServerEval().javascript(content);
        } else if (context.optionExists(callOptions.xqueryFileOptionName)) {
            String content = readFileToString(properties.get(callOptions.xqueryFileOptionName));
            return client -> client.newServerEval().xquery(content);
        }
        return null;
    }

    public ServerEvaluationCall buildCall(DatabaseClient client) {
        if (callBuilder == null) {
            return null;
        }

        ServerEvaluationCall call = callBuilder.apply(client);
        userDefinedVariables.forEach(call::addVariable);
        return call;
    }

    private static String readFileToString(String file) {
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

    // Intended solely to simplify passing these 3 option names around.
    public static class CallOptions {
        public String invokeOptionName;
        public final String javascriptOptionName;
        public final String xqueryOptionName;
        public final String javascriptFileOptionName;
        public final String xqueryFileOptionName;

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
