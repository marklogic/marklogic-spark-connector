/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.reader.optic.SchemaInferrer;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Map;

class GenerateTdeTest extends AbstractIntegrationTest {

    /**
     * We can inject this somewhere into the writer before any partitions are
     * created. Needs to be somewhere where we know it will only be executed once. Possibly
     * DefaultSource then?
     * <p>
     * Seems best if we know the name of the schemas database. Can use v1/documents.
     * Could also do an eval which is probably better too?
     *
     * We'd need the following info from a user - I don't think there's any way for us to generate a reasonable
     * schema/view when importing from something like a Parquet file.
     *
     * schema
     * view
     *
     * For permissions = I think we can use whatever the "write permissions" option is for now.
     * For a URI - how about if we concatenate the schema and view?
     *
     * So:
     * spark.marklogic.write.tde.schema=
     * spark.marklogic.write.tde.view=
     *
     * For the collections - I think we can use whatever the first collection is in "write collections".
     * @throws Exception
     */
    @Test
    void test() throws Exception {
        StructType schema = newSparkSession().read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("src/test/resources/csv-files/empty-values.csv")
            .schema();

        System.out.println(schema.prettyJson());

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode tde = mapper.createObjectNode();
        ObjectNode template = tde.putObject("template");
        template.put("context", "/");
        template.putArray("collections").add("testing");
        // TODO Can take collections as an input?
        ObjectNode row = template.putArray("rows").addObject();
        row.put("schemaName", "todo");
        row.put("viewName", "changeme");
        ArrayNode columns = row.putArray("columns");
        for (StructField field : schema.fields()) {
            ObjectNode column = columns.addObject();
            column.put("name", field.name());
            column.put("val", field.name());
            String type = getTdeType(field.dataType());
            column.put("scalarType", type);
        }

        String eval = "declareUpdate();\n" +
            "const tde = require('/MarkLogic/tde.xqy');\n" +
            String.format("const template = xdmp.toJSON(%s);\n", tde) +
            "tde.templateInsert('/a.json', template)";
        getDatabaseClient().newServerEval().javascript(eval).evalAs(String.class);
    }

    private String getTdeType(DataType dataType) {
        for (Map.Entry<String, DataType> entry : SchemaInferrer.COLUMN_INFO_TYPES_TO_SPARK_TYPES.entrySet()) {
            if (entry.getValue().sameType(dataType)) {
                return entry.getKey();
            }
        }
        // Could log a warning here.
        return "string";
    }
}
