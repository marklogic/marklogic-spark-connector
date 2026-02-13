/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MetadataEntryNameTest {

    @Test
    void parseNewFormatWithBinaryFormat() {
        MetadataEntryName result = MetadataEntryName.parse("/doc3.xml.BINARY.metadata", "/doc3.xml");
        assertEquals("/doc3.xml", result.documentUri());
        assertEquals("BINARY", result.format());
    }

    @Test
    void parseNewFormatWithJsonFormat() {
        MetadataEntryName result = MetadataEntryName.parse("/author/data.json.JSON.metadata", "/author/data.json");
        assertEquals("/author/data.json", result.documentUri());
        assertEquals("JSON", result.format());
    }

    @Test
    void oldFormatWithJSONInName() {
        MetadataEntryName result = MetadataEntryName.parse("/author/data.JSON.metadata", "/author/data.JSON");
        assertEquals("/author/data.JSON", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseOldFormatWithJsonExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/author/1.json.metadata", "/author/1.json");
        assertEquals("/author/1.json", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseOldFormatWithXmlExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/doc.xml.metadata", "/doc.xml");
        assertEquals("/doc.xml", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseOldFormatWithTextExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/readme.txt.metadata", "/readme.txt");
        assertEquals("/readme.txt", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseNewFormatWithText() {
        MetadataEntryName result = MetadataEntryName.parse("/readme.txt.TEXT.metadata", "/readme.txt");
        assertEquals("/readme.txt", result.documentUri());
        assertEquals("TEXT", result.format());
    }

    @Test
    void parseOldFormatWithBinaryExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/file.binary.metadata", "/file.binary");
        assertEquals("/file.binary", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseNewFormatWithBinary() {
        MetadataEntryName result = MetadataEntryName.parse("/doc.xml.BINARY.metadata", "/doc.xml");
        assertEquals("/doc.xml", result.documentUri());
        assertEquals("BINARY", result.format());
    }

    @Test
    void parseOldFormatNoKnownExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/doc.metadata", "/doc");
        assertEquals("/doc", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void parseOldFormatWithOtherExtension() {
        MetadataEntryName result = MetadataEntryName.parse("/file.pdf.metadata", "/file.pdf");
        assertEquals("/file.pdf", result.documentUri());
        assertNull(result.format());
    }

    @Test
    void makeMetadataEntryNameWithFormat() {
        MetadataEntryName entry = new MetadataEntryName("/doc3.xml", "BINARY");
        assertEquals("/doc3.xml.BINARY.metadata", entry.makeMetadataEntryName());
    }

    @Test
    void makeMetadataEntryNameWithoutFormat() {
        MetadataEntryName entry = new MetadataEntryName("/doc.xml", null);
        assertEquals("/doc.xml.metadata", entry.makeMetadataEntryName());
    }

    @Test
    void makeMetadataEntryNameWithEmptyFormat() {
        MetadataEntryName entry = new MetadataEntryName("/doc.xml", "");
        assertEquals("/doc.xml.metadata", entry.makeMetadataEntryName());
    }

    @Test
    void caseInsensitiveFormatParsing() {
        MetadataEntryName result = MetadataEntryName.parse("/doc.xml.binary.metadata", "/doc.xml");
        assertEquals("/doc.xml", result.documentUri());
        assertEquals("BINARY", result.format());
    }
}
