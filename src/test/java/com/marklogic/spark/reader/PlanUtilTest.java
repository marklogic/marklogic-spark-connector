package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PlanUtilTest {

    @Test
    void buildSelectWithNoQualifier() {
        ObjectNode select = PlanUtil.buildSelect(new StructType()
            .add("myField", DataTypes.StringType));

        JsonNode schemaColArgs = select.get("args").get(0).get(0).get("args");
        assertEquals(3, schemaColArgs.size());
        assertEquals(JsonNodeType.NULL, schemaColArgs.get(0).getNodeType());
        assertEquals(JsonNodeType.NULL, schemaColArgs.get(1).getNodeType());
        assertEquals("myField", schemaColArgs.get(2).asText());
    }

    @Test
    void buildSelectWithViewQualifier() {
        ObjectNode select = PlanUtil.buildSelect(new StructType()
            .add("myView.myField", DataTypes.StringType));

        JsonNode schemaColArgs = select.get("args").get(0).get(0).get("args");
        assertEquals(3, schemaColArgs.size());
        assertEquals(JsonNodeType.NULL, schemaColArgs.get(0).getNodeType());
        assertEquals("myView", schemaColArgs.get(1).asText());
        assertEquals("myField", schemaColArgs.get(2).asText());
    }

    @Test
    void buildSelectWithSchemaAndViewQualifiers() {
        ObjectNode select = PlanUtil.buildSelect(new StructType()
            .add("mySchema.myView.myField", DataTypes.StringType));

        JsonNode schemaColArgs = select.get("args").get(0).get(0).get("args");
        assertEquals(3, schemaColArgs.size());
        assertEquals("mySchema", schemaColArgs.get(0).asText());
        assertEquals("myView", schemaColArgs.get(1).asText());
        assertEquals("myField", schemaColArgs.get(2).asText());
    }
}
