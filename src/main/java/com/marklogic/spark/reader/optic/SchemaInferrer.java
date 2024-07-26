/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public abstract class SchemaInferrer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // "Column info types" = the possible set of types returned by the columnInfo call to /v1/rows. Note that this is
    // not equivalent to the set of TDE types; for example, /v1/rows returns "none" as a column type for several TDE types.
    private static final Map<String, DataType> COLUMN_INFO_TYPES_TO_SPARK_TYPES = new HashMap<>();
    static {
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("int", DataTypes.IntegerType);
        // Including "short" here, but a TDE column of type "short" reports "int" as its type in column info. So not
        // yet able to test this, but including it here in case the server does report "short" in the future.
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("short", DataTypes.ShortType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("unsignedInt", DataTypes.IntegerType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("long", DataTypes.LongType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("unsignedLong", DataTypes.LongType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("float", DataTypes.FloatType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("double", DataTypes.DoubleType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("decimal", DataTypes.DoubleType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("dateTime", DataTypes.TimestampType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("date", DataTypes.DateType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("gYearMonth", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("gYear", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("gMonth", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("gDay", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("yearMonthDuration", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("dayTimeDuration", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("string", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("anyUri", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("point", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("boolean", DataTypes.BooleanType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("none", DataTypes.StringType); // See DBQ-296, this is intentional for some column types.
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put ("value", DataTypes.StringType); // In MarkLogic 10, "value" is returned for a column containing a JSON object.
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("integer", DataTypes.IntegerType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("unsignedInt", DataTypes.IntegerType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("iri", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("time", DataTypes.StringType);
        COLUMN_INFO_TYPES_TO_SPARK_TYPES.put("unknown", DataTypes.StringType);
    }

    private SchemaInferrer() {}

    /**
     * @param columnInfoResponse the String response from a call to /v1/rows with output=columnInfo; the endpoint
     *                           returns a String of newline-delimited JSON objects
     * @return
     */
    public static StructType inferSchema(String columnInfoResponse) {
        StructType schema = new StructType();
        for (String columnInfo : columnInfoResponse.split("\n")) {
            try {
                JsonNode column = objectMapper.readTree(columnInfo);
                if (ignoreColumn(column)) {
                    continue;
                }
                schema = schema.add(makeColumnName(column), determineSparkType(column), isColumnNullable(column));
            } catch (JsonProcessingException e) {
                throw new ConnectorException(String.format("Unable to parse schema JSON: %s; cause: %s", columnInfo, e.getMessage()), e);
            }
        }
        return schema;
    }

    private static boolean ignoreColumn(JsonNode column) {
        // MarkLogic 11 defines some columns, such as "rowid", as "hidden".
        if (column.has("hidden") && Boolean.TRUE.equals(column.get("hidden").asBoolean())) {
            return true;
        }
        // In MarkLogic 10, "hidden" doesn't exist, so need to explicitly check for rowid
        return "rowid".equals(column.get("type").asText());
    }

    private static DataType determineSparkType(JsonNode column) {
        final String type = column.get("type").asText();
        if (COLUMN_INFO_TYPES_TO_SPARK_TYPES.containsKey(type)) {
            return COLUMN_INFO_TYPES_TO_SPARK_TYPES.get(type);
        }
        Util.MAIN_LOGGER.warn("Unrecognized column type: {}; will map to Spark StringType", column);
        return DataTypes.StringType;
    }

    private static String makeColumnName(JsonNode column) {
        StringBuilder builder = new StringBuilder();
        appendIfNotEmpty(column, "schema", builder);
        appendIfNotEmpty(column, "view", builder);
        builder.append(column.get("column").asText());
        return builder.toString();
    }

    private static void appendIfNotEmpty(JsonNode column, String columnName, StringBuilder builder) {
        if (column.has(columnName)) {
            String value = column.get(columnName).asText();
            if (value != null && value.trim().length() > 0) {
                builder.append(value).append(".");
            }
        }
    }

    private static boolean isColumnNullable(JsonNode column) {
        return column.has("nullable") && column.get("nullable").asBoolean();
    }
}
