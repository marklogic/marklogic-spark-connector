/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;

import java.util.List;
import java.util.function.Consumer;

public class OpticUrisProcessor implements Consumer<List<String>> {

    private final DatabaseClient databaseClient;
    private final String patch;

    public OpticUrisProcessor(DatabaseClient databaseClient, String patch) {
        this.databaseClient = databaseClient;
        this.patch = patch;
    }

    public void accept(List<String> uris) {
        StringBuilder opticQuery = new StringBuilder("op.fromParam('uris', '', [{\"column\":\"uri\", \"type\":\"string\"}])\n");
        opticQuery.append(".joinDocCols(null, op.col('uri'))\n");
        opticQuery.append(String.format(".patch(op.col('doc'), %s)\n", patch));
        opticQuery.append(".write()");

        ArrayNode array = new ObjectMapper().createArrayNode();
        uris.forEach(uri -> array.addObject().put("uri", uri));
//        uris.forEach(array::add);

        RowManager rowManager = databaseClient.newRowManager().withUpdate(true);
        PlanBuilder.Plan plan = rowManager.newRawQueryDSLPlan(new StringHandle(opticQuery.toString()))
            .bindParam("uris", new JacksonHandle(array));
        rowManager.execute(plan);
    }
}
