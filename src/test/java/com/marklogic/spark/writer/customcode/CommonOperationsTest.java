/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CommonOperationsTest extends AbstractIntegrationTest {

    @BeforeEach
    void setup() {
        JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        DocumentWriteSet set = mgr.newWriteSet();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections("temp");
        metadata.getPermissions().addFromDelimitedString(DEFAULT_PERMISSIONS);
        metadata.getPermissions().add("qconsole-user", DocumentMetadataHandle.Capability.READ);
        ObjectNode doc = objectMapper.createObjectNode().put("hello", "world");
        set.add("/atemp1.json", metadata, new JacksonHandle(doc));
        set.add("/atemp2.json", metadata, new JacksonHandle(doc));
        mgr.write(set);
    }

    @Test
    void test() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_JAVASCRIPT, "cts.uris(null, null, cts.collectionQuery('temp'))")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URIS_COLLECTIONS_ADD, "c1")
            .option(Options.WRITE_URIS_COLLECTIONS_REMOVE, "temp")
            .option(Options.WRITE_URIS_PERMISSIONS_ADD, "qconsole-user,update")
            .option(Options.WRITE_URIS_PERMISSIONS_REMOVE, "qconsole-user,read")
//            .option(Options.WRITE_URIS_COLLECTIONS_REMOVE, "temp")
//            .option(Options.WRITE_URIS_COLLECTIONS_SET, "c2,c3")
            .option(Options.CLIENT_URI, makeClientUri())
            .mode(SaveMode.Append)
            .save();
    }

    @Test
    void patch() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_JAVASCRIPT, "cts.uris(null, null, cts.collectionQuery('temp'))")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URIS_PATCH, "op.patchBuilder('/').replaceValue('hello', 'modified')")
            .option(Options.CLIENT_URI, makeClientUri())
            .mode(SaveMode.Append)
            .save();
    }
}
