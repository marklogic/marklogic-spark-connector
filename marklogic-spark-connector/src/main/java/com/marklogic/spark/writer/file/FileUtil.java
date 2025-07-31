/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.hadoop.fs.Path;

public interface FileUtil {

    static Path makePathFromDocumentURI(String basePath, String documentURI) {
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
