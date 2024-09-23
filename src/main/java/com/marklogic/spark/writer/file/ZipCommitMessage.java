/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

class ZipCommitMessage implements WriterCommitMessage {

    private final String path;
    private final int zipEntryCount;

    ZipCommitMessage(String path, int zipEntryCount) {
        this.path = path;
        this.zipEntryCount = zipEntryCount;
    }

    String getPath() {
        return path;
    }

    int getZipEntryCount() {
        return zipEntryCount;
    }
}
