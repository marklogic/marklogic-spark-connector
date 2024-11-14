/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

/**
 * Used when streaming from a zip file or archive file.
 */
public enum StreamingMode {

    STREAM_DURING_READER_PHASE,
    STREAM_DURING_WRITER_PHASE
    
}
