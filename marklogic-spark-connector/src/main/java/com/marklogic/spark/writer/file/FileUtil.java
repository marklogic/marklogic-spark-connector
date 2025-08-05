/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.Util;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public interface FileUtil {

    static Path makePathFromDocumentURI(String basePath, String documentURI) {
        // Mostly copied from MLCP.
        try {
            URI uri = new URI(documentURI);
            // As of 1.4.0, an opaque URI will not cause a problem due to the inclusion of "./" below.
            // We still parse the URI to get the path in case there is a scheme (e.g. file://).
            if (!uri.isOpaque()) {
                documentURI = uri.getPath();
            }
        } catch (URISyntaxException e) {
            // MLCP logs errors from parsing the URI at the "WARN" level. That seems noisy, as large numbers of URIs
            // could e.g. have spaces in them. So DEBUG is used instead.
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Unable to parse document URI: {}; will use unparsed URI as file path.", documentURI);
            }
        }

        if (documentURI.charAt(0) == '/') {
            return new Path(basePath + documentURI);
        }
        // Updated in 2.7.0 based a bug involving multiple colons and no leading slash. Copilot noted that we can just
        // prepend the URI with "./" which will make the Hadoop Path constructor happy. That also means we no longer
        // chop off the text before the first colon, which was happening in the previous version. While this is a
        // change in behavior, it's a change for the better as it could also be considered that the behavior of
        // chopping off text as a bug (though it was copy/pasted from the MLCP codebase).
        return new Path(basePath, "./" + documentURI);
    }
}
