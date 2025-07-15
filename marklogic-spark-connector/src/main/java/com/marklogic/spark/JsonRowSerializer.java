/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.types.StructType;
import scala.Predef;
import scala.collection.JavaConverters;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles serializing a Spark row into a JSON string. Includes support for all the options defined in Spark's
 * JSONOptions.scala class.
 */
public class JsonRowSerializer {

    private final StructType schema;
    private final JSONOptions jsonOptions;
    private final boolean includeNullFields;

    public JsonRowSerializer(StructType schema, Map<String, String> connectorProperties) {
        this.schema = schema;

        final Map<String, String> options = buildOptionsForJsonOptions(connectorProperties);
        this.includeNullFields = "false".equalsIgnoreCase(options.get("ignoreNullFields"));

        this.jsonOptions = new JSONOptions(
            // Funky code to convert a Java map into a Scala immutable Map.
            JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(Predef.$conforms()),

            // As verified via tests, this default timezone ID is overridden by a user via
            // the spark.sql.session.timeZone option.
            "Z",

            // We don't expect corrupted records - i.e. corrupted values - to be present in the index. But Spark
            // requires this to be set. See
            // https://medium.com/@sasidharan-r/how-to-handle-corrupt-or-bad-record-in-apache-spark-custom-logic-pyspark-aws-430ddec9bb41
            // for more information.
            "_corrupt_record"
        );
    }

    public String serializeRowToJson(InternalRow row) {
        StringWriter writer = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(this.schema, writer, this.jsonOptions);
        jacksonGenerator.write(row);
        jacksonGenerator.flush();
        return writer.toString();
    }

    /**
     * A user can specify any of the options found in the JSONOptions.scala class - though it's not yet clear where
     * a user finds out about these except via the Spark source code. "ignoreNullFields" however is expected to be the
     * primary one that is configured.
     */
    private Map<String, String> buildOptionsForJsonOptions(Map<String, String> connectorProperties) {
        Map<String, String> options = new HashMap<>();
        // Default to include null fields, as they are easily queried in MarkLogic.
        options.put("ignoreNullFields", "false");
        connectorProperties.forEach((key, value) -> {
            if (key.startsWith(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX)) {
                String optionName = key.substring(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX.length());
                options.put(optionName, value);
            }
        });
        return options;
    }

    public JSONOptions getJsonOptions() {
        return jsonOptions;
    }

    public boolean isIncludeNullFields() {
        return this.includeNullFields;
    }
}
