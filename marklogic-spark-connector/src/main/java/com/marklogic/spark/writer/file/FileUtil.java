/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.Util;

import java.net.URI;
import java.net.URISyntaxException;

public interface FileUtil {

    static String makePathFromDocumentURI(String documentURI) {
        // Mostly copied from MLCP.
        try {
            URI uri = new URI(documentURI);
            // The isOpaque check is made because an opaque URI will not have a path.
            return uri.isOpaque() ? uri.getSchemeSpecificPart() : uri.getPath();
        } catch (URISyntaxException e) {
            // MLCP logs errors from parsing the URI at the "WARN" level. That seems noisy, as large numbers of URIs
            // could e.g. have spaces in them. So DEBUG is used instead.
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Unable to parse document URI: {}; will use unparsed URI as file path.", documentURI);
            }
            return documentURI;
        }
    }
}
