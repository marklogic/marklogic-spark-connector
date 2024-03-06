package com.marklogic.spark.writer.file;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

class FileCommitMessage implements WriterCommitMessage {

    private final String path;
    private final int fileCount;

    FileCommitMessage(String path, int fileCount) {
        this.path = path;
        this.fileCount = fileCount;
    }

    String getPath() {
        return path;
    }

    int getFileCount() {
        return fileCount;
    }
}
