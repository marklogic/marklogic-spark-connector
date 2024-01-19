package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileUtilTest {

    @Test
    void makePathFromRegularURI() {
        String uri = FileUtil.makePathFromDocumentURI("/path/to/doc.xml");
        assertEquals("/path/to/doc.xml", uri);
    }

    @Test
    void makePathFromOpaqueURI() {
        String uri = FileUtil.makePathFromDocumentURI("org:example:123.xml");
        assertEquals("example:123.xml", uri);
    }

    @Test
    void makePathWithInvalidURI() {
        ConnectorException ex = assertThrows(ConnectorException.class, () -> FileUtil.makePathFromDocumentURI(":::"));
        assertEquals("Unable to construct URI from: :::", ex.getMessage());
    }
}
