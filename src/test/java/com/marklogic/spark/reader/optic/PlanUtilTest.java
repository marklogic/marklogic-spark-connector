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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PlanUtilTest {

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
