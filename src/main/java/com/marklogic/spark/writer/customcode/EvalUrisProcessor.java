/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.JacksonHandle;

import java.util.List;
import java.util.function.Consumer;

/**
 * The trick with using Optic Update for this is that we have to fetch collections/permissions and then
 * implement add/remove ourselves. Whereas with an eval, we can just call the xdmp functions that do it already.
 */
public class EvalUrisProcessor implements Consumer<List<String>> {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String[] collectionsToAdd;
    private String[] collectionsToRemove;
    private String[] collectionsToSet;
    private String[] permissionsToAdd;
    private String[] permissionsToRemove;
    private String[] permissionsToSet;

    public EvalUrisProcessor(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public void accept(List<String> uris) {
        ServerEvaluationCall eval = databaseClient.newServerEval();
        ArrayNode urisArray = objectMapper.createArrayNode();
        uris.forEach(urisArray::add);
        eval.addVariable("URIS", new JacksonHandle(urisArray));

        StringBuilder script = new StringBuilder("declareUpdate();\nfor (var uri of URIS) {\n");
        applyCollectionsToAdd(script, eval);
        applyCollectionsToRemove(script, eval);
        applyCollectionsToSet(script, eval);
        applyPermissionsToAdd(script, eval);
        applyPermissionsToRemove(script, eval);
        script.append("\n}");
        eval.javascript(script.toString()).evalAs(String.class);
    }

    private void applyCollectionsToAdd(StringBuilder script, ServerEvaluationCall eval) {
        if (collectionsToAdd != null && collectionsToAdd.length > 0) {
            script.append("xdmp.documentAddCollections(uri, COLLECTIONS_TO_ADD);\n");
            eval.addVariable("COLLECTIONS_TO_ADD", toHandle(collectionsToAdd));
        }
    }

    private void applyCollectionsToRemove(StringBuilder script, ServerEvaluationCall eval) {
        if (collectionsToRemove != null && collectionsToRemove.length > 0) {
            script.append("xdmp.documentRemoveCollections(uri, COLLECTIONS_TO_REMOVE);\n");
            eval.addVariable("COLLECTIONS_TO_REMOVE", toHandle(collectionsToRemove));
        }
    }

    private void applyCollectionsToSet(StringBuilder script, ServerEvaluationCall eval) {
        if (collectionsToSet != null && collectionsToSet.length > 0) {
            script.append("xdmp.documentSetCollections(uri, COLLECTIONS_TO_SET);\n");
            eval.addVariable("COLLECTIONS_TO_SET", toHandle(collectionsToSet));
        }
    }

    private void applyPermissionsToAdd(StringBuilder script, ServerEvaluationCall eval) {
        if (permissionsToAdd != null && permissionsToAdd.length > 0) {
            eval.addVariable("PERMISSIONS_TO_ADD", toHandle(permissionsToAdd));
            addPermissionsBlock(script, "permissionsToAdd", "PERMISSIONS_TO_ADD", "documentAddPermissions");
        }
    }

    private void applyPermissionsToRemove(StringBuilder script, ServerEvaluationCall eval) {
        if (permissionsToRemove != null && permissionsToRemove.length > 0) {
            eval.addVariable("PERMISSIONS_TO_REMOVE", toHandle(permissionsToRemove));
            addPermissionsBlock(script, "permissionsToRemove", "PERMISSIONS_TO_REMOVE", "documentRemovePermissions");
        }
    }

    private void applyPermissionsToSet(StringBuilder script, ServerEvaluationCall eval) {
        if (permissionsToSet != null && permissionsToSet.length > 0) {
            eval.addVariable("PERMISSIONS_TO_SET", toHandle(permissionsToSet));
            addPermissionsBlock(script, "permissionsToSet", "PERMISSIONS_TO_SET", "documentSetPermissions");
        }
    }

    private void addPermissionsBlock(StringBuilder script, String arrayName, String inputName, String functionName) {
        script.append(String.format("const %s = [];\n", arrayName));
        script.append(String.format("for (var i = 0; i < %s.length; i += 2) {\n", inputName));
        script.append(String.format("  %s.push(xdmp.permission(%s[i], %s[i+1]));\n", arrayName, inputName, inputName));
        script.append(String.format("}\nxdmp.%s(uri, %s);\n", functionName, arrayName));
    }

    private JacksonHandle toHandle(String[] values) {
        ArrayNode array = objectMapper.createArrayNode();
        for (String s : values) {
            array.add(s);
        }
        return new JacksonHandle(array);
    }

    public EvalUrisProcessor withCollectionsToAdd(String... collectionsToAdd) {
        this.collectionsToAdd = collectionsToAdd;
        return this;
    }

    public EvalUrisProcessor withCollectionsToRemove(String... collectionsToRemove) {
        this.collectionsToRemove = collectionsToRemove;
        return this;
    }

    public EvalUrisProcessor withCollectionsToSet(String... collectionsToSet) {
        this.collectionsToSet = collectionsToSet;
        return this;
    }

    public EvalUrisProcessor withPermissionsToAdd(String... permissionsToAdd) {
        this.permissionsToAdd = permissionsToAdd;
        return this;
    }

    public EvalUrisProcessor withPermissionsToRemove(String... permissionsToRemove) {
        this.permissionsToRemove = permissionsToRemove;
        return this;
    }

    public EvalUrisProcessor withPermissionsToSet(String... permissionsToSet) {
        this.permissionsToSet = permissionsToSet;
        return this;
    }
}
