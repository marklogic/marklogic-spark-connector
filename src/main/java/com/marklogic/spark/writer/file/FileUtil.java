package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;

import java.net.URI;
import java.net.URISyntaxException;

abstract class FileUtil {

    static String makePathFromDocumentURI(String documentURI) {
        // Copied from MLCP
        URI uri;
        try {
            uri = new URI(documentURI);
        } catch (URISyntaxException e) {
            throw new ConnectorException(String.format("Unable to construct URI from: %s", documentURI), e);
        }
        // The isOpaque check is made because an opaque URI will not have a path.
        return uri.isOpaque() ? uri.getSchemeSpecificPart() : uri.getPath();
    }

    private FileUtil() {
    }
}
