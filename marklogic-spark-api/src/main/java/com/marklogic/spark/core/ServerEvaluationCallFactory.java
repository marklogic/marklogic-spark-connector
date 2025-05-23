/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Facilitates building a {@code ServerEvaluationCall} based on several options. Potentially reusable outside Spark,
 * as it has no Spark dependencies and only depends on a map of properties.
 */
public class ServerEvaluationCallFactory {

    private final Function<DatabaseClient, ServerEvaluationCall> callBuilder;
    private final Map<String, String> userDefinedVariables;

    private ServerEvaluationCallFactory(Function<DatabaseClient, ServerEvaluationCall> callBuilder, Map<String, String> userDefinedVariables) {
        this.callBuilder = Objects.requireNonNull(callBuilder);
        this.userDefinedVariables = userDefinedVariables;
    }

    public ServerEvaluationCall newCall(DatabaseClient client) {
        ServerEvaluationCall call = callBuilder.apply(client);
        if (userDefinedVariables != null) {
            userDefinedVariables.forEach(call::addVariable);
        }
        return call;
    }

    public static class Builder {

        private String invokeOptionName;
        private String javascriptOptionName;
        private String xqueryOptionName;
        private String javascriptFileOptionName;
        private String xqueryFileOptionName;
        private String varsPrefix;

        /**
         * For scenarios where the user is not required to specify options for building a call.
         */
        public Optional<ServerEvaluationCallFactory> build(Context context) {
            Function<DatabaseClient, ServerEvaluationCall> callBuilder = buildFunction(context);
            if (callBuilder != null) {
                Map<String, String> userDefinedVariables = varsPrefix != null ?
                    buildUserDefinedVariables(context) : null;
                return Optional.of(new ServerEvaluationCallFactory(callBuilder, userDefinedVariables));
            }
            return Optional.empty();
        }

        /**
         * For scenarios where the user is required to specify options for building a call.
         */
        public ServerEvaluationCallFactory mustBuild(Context context) {
            Optional<ServerEvaluationCallFactory> builder = build(context);
            if (builder.isEmpty()) {
                throw new ConnectorException("Must specify one of the following options: " + Arrays.asList(
                    invokeOptionName, javascriptOptionName, xqueryOptionName,
                    javascriptFileOptionName, xqueryFileOptionName
                ));
            }
            return builder.get();
        }

        public Builder withInvokeOptionName(String invokeOptionName) {
            this.invokeOptionName = invokeOptionName;
            return this;
        }

        public Builder withJavascriptOptionName(String javascriptOptionName) {
            this.javascriptOptionName = javascriptOptionName;
            return this;
        }

        public Builder withXqueryOptionName(String xqueryOptionName) {
            this.xqueryOptionName = xqueryOptionName;
            return this;
        }

        public Builder withJavascriptFileOptionName(String javascriptFileOptionName) {
            this.javascriptFileOptionName = javascriptFileOptionName;
            return this;
        }

        public Builder withXqueryFileOptionName(String xqueryFileOptionName) {
            this.xqueryFileOptionName = xqueryFileOptionName;
            return this;
        }

        public Builder withVarsPrefix(String varsPrefix) {
            this.varsPrefix = varsPrefix;
            return this;
        }

        private Function<DatabaseClient, ServerEvaluationCall> buildFunction(Context context) {
            // Cannot recall why the property map is accessed, may be due to case-sensitive values?
            final Map<String, String> properties = context.getProperties();
            if (context.hasOption(this.invokeOptionName)) {
                String path = properties.get(this.invokeOptionName);
                return client -> client.newServerEval().modulePath(path);
            } else if (context.hasOption(this.javascriptOptionName)) {
                String javascript = properties.get(this.javascriptOptionName);
                return client -> client.newServerEval().javascript(javascript);
            } else if (context.hasOption(this.xqueryOptionName)) {
                String xquery = properties.get(this.xqueryOptionName);
                return client -> client.newServerEval().xquery(xquery);
            } else if (context.hasOption(this.javascriptFileOptionName)) {
                String content = readFileToString(properties.get(this.javascriptFileOptionName));
                return client -> client.newServerEval().javascript(content);
            } else if (context.hasOption(this.xqueryFileOptionName)) {
                String content = readFileToString(properties.get(this.xqueryFileOptionName));
                return client -> client.newServerEval().xquery(content);
            }
            return null;
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

        private Map<String, String> buildUserDefinedVariables(Context context) {
            Map<String, String> properties = context.getProperties();
            return properties.keySet().stream()
                .filter(key -> key.startsWith(varsPrefix))
                .collect(Collectors.toMap(
                    key -> key.substring(varsPrefix.length()),
                    properties::get
                ));
        }
    }

}
