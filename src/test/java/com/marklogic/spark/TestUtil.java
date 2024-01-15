package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;

import javax.xml.namespace.QName;

public interface TestUtil {

    static void insertTwoDocumentsWithAllMetadata(DatabaseClient client) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.setQuality(10);

        metadata.getCollections().addAll("collection1", "collection2");

        metadata.getPermissions().add("spark-user-role", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);
        metadata.getPermissions().add("qconsole-user", DocumentMetadataHandle.Capability.READ);

        metadata.getProperties().put(new QName("org:example", "key1"), "value1");
        metadata.getProperties().put(QName.valueOf("key2"), "value2");

        metadata.getMetadataValues().put("meta1", "value1");
        metadata.getMetadataValues().put("meta2", "value2");

        DocumentWriteSet writeSet = client.newDocumentManager().newWriteSet();
        for (int i = 1; i <= 2; i++) {
            writeSet.add("/test/" + i + ".xml", metadata, new StringHandle("<hello>world</hello>"));
        }
        client.newDocumentManager().write(writeSet);
    }
}
