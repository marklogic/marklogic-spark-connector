package com.marklogic.spark.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

abstract class SchemaInferrer {

    private final static Logger logger = LoggerFactory.getLogger(SchemaInferrer.class);
    private final static ObjectMapper objectMapper = new ObjectMapper();

    // "Column info types" = the possible set of types returned by the columnInfo call to /v1/rows. Note that this is
    // not equivalent to the set of TDE types; for example, /v1/rows returns "none" as a column type for several TDE types.
    private final static Map<String, DataType> COLUMN_INFO_TYPES_TO_SPARK_TYPES = new HashMap() {{
        put("int", DataTypes.IntegerType);
        put("unsignedInt", DataTypes.IntegerType);
        put("long", DataTypes.LongType);
        put("unsignedLong", DataTypes.LongType);
        put("float", DataTypes.FloatType);
        put("double", DataTypes.DoubleType);
        put("decimal", DataTypes.DoubleType);
        put("dateTime", DataTypes.TimestampType);
        put("date", DataTypes.DateType);
        put("gYearMonth", DataTypes.StringType);
        put("gYear", DataTypes.StringType);
        put("gMonth", DataTypes.StringType);
        put("gDay", DataTypes.StringType);
        put("yearMonthDuration", DataTypes.StringType);
        put("dayTimeDuration", DataTypes.StringType);
        put("string", DataTypes.StringType);
        put("anyUri", DataTypes.StringType);
        put("point", DataTypes.StringType);
        put("boolean", DataTypes.BooleanType);
        put("none", DataTypes.StringType); // See DBQ-296, this is intentional for some column types.
        put("integer", DataTypes.IntegerType);
        put("unsignedInt", DataTypes.IntegerType);
        put("iri", DataTypes.StringType);
        put("time", DataTypes.StringType);
    }};

    /**
     * @param columnInfoResponse the String response from a call to /v1/rows with output=columnInfo; the endpoint
     *                           returns a String of newline-delimited JSON objects
     * @return
     */
    static StructType inferSchema(String columnInfoResponse) {
        StructType schema = new StructType();
        for (String columnInfo : columnInfoResponse.split("\n")) {
            try {
                JsonNode column = objectMapper.readTree(columnInfo);
                // "rowid" is an example of a hidden column.
                if (column.has("hidden") && Boolean.TRUE.equals(column.get("hidden").asBoolean())) {
                    continue;
                }
                schema = schema.add(makeColumnName(column), determineSparkType(column), isColumnNullable(column));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(String.format("Unable to parse JSON: %s; cause: %s", columnInfo, e.getMessage()), e);
            }
        }
        return schema;
    }

    private static DataType determineSparkType(JsonNode column) {
        final String type = column.get("type").asText();
        if (COLUMN_INFO_TYPES_TO_SPARK_TYPES.containsKey(type)) {
            return COLUMN_INFO_TYPES_TO_SPARK_TYPES.get(type);
        }
        logger.warn("Unrecognized column type: {}; will map to Spark StringType", type);
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
        return column.has("nullable") ? column.get("nullable").asBoolean() : true;
    }
}
