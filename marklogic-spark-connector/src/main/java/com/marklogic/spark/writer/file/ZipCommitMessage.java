/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

class ZipCommitMessage implements WriterCommitMessage {

    private final String path;
    private final String zipFilePath;
    private final int zipEntryCount;

    ZipCommitMessage(String path, String zipFilePath, int zipEntryCount) {
        this.path = path;
        this.zipFilePath = zipFilePath;
        this.zipEntryCount = zipEntryCount;
    }

    String getPath() {
        return path;
    }

    String getZipFilePath() {
        return zipFilePath;
    }

    int getZipEntryCount() {
        return zipEntryCount;
    }
}
