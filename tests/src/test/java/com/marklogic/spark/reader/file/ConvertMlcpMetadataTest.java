/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.junit5.PermissionsTester;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import javax.xml.namespace.QName;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConvertMlcpMetadataTest {

    @Test
    void test() throws Exception {
        InputStream input = new ClassPathResource("mlcp-metadata/complete.xml").getInputStream();
        MlcpMetadata mlcpMetadata = new MlcpMetadataConverter().convert(input);
        assertEquals(Format.XML, mlcpMetadata.getFormat());

        DocumentMetadataHandle metadata = mlcpMetadata.getMetadata();

        assertEquals(2, metadata.getCollections().size());
        assertTrue(metadata.getCollections().contains("collection1"));
        assertTrue(metadata.getCollections().contains("collection2"));

        assertEquals(10, metadata.getQuality());

        assertEquals(2, metadata.getMetadataValues().size());
        assertEquals("value1", metadata.getMetadataValues().get("meta1"));
        assertEquals("value2", metadata.getMetadataValues().get("meta2"));

        assertEquals(2, metadata.getProperties().size());
        assertEquals("value1", metadata.getProperties().get(new QName("org:example", "key1")));
        assertEquals("value2", metadata.getProperties().get("key2"));

        assertEquals(2, metadata.getPermissions().size());
        PermissionsTester tester = new PermissionsTester(metadata.getPermissions());
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
    }
}
