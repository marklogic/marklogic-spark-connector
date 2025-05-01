/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.rdf;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Knows how to use the non-public "sem:create-graph-document(iri, permissions)" function to write sem:graph
 * documents.
 */
public class GraphWriter {

    private static final Logger logger = LoggerFactory.getLogger(GraphWriter.class);

    private final DatabaseClient databaseClient;
    private final String permissions;

    public GraphWriter(DatabaseClient databaseClient, String rolesAndCapabilities) {
        this.databaseClient = databaseClient;
        this.permissions = rolesAndCapabilities != null && !rolesAndCapabilities.trim().isEmpty() ?
            parsePermissions(rolesAndCapabilities) :
            "xdmp:default-permissions()";
    }

    public void createGraphs(Set<String> graphs) {
        for (String graph : graphs) {
            StringBuilder query = new StringBuilder("declare variable $GRAPH external; ");
            query.append(String.format(
                "if (fn:doc-available($GRAPH)) then () else sem:create-graph-document(sem:iri($GRAPH), %s)",
                permissions)
            );

            if (logger.isDebugEnabled()) {
                logger.debug("Writing graph {} if it does not yet exist.", graph);
            }
            ServerEvaluationCall call = databaseClient.newServerEval().xquery(query.toString());
            call.addVariable("GRAPH", graph);
            call.evalAs(String.class);
        }
    }

    /**
     * We know the permissions string is valid at this point, as if it weren't, the writing process would have failed
     * before the connector gets to here.
     *
     * @param rolesAndCapabilities
     * @return
     */
    private String parsePermissions(final String rolesAndCapabilities) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        Util.addPermissionsFromDelimitedString(metadata.getPermissions(), rolesAndCapabilities);
        StringBuilder permissionsString = new StringBuilder("(");
        boolean firstOne = true;
        for (String role : metadata.getPermissions().keySet()) {
            for (DocumentMetadataHandle.Capability cap : metadata.getPermissions().get(role)) {
                if (!firstOne) {
                    permissionsString.append(", ");
                }
                permissionsString.append(String.format("xdmp:permission('%s', '%s')", role, cap.toString().toLowerCase()));
                firstOne = false;
            }
        }
        return permissionsString.append(")").toString();
    }
}
