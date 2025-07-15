/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

/**
 * Used when streaming from a zip file or archive file.
 */
public enum StreamingMode {

    STREAM_DURING_READER_PHASE,
    STREAM_DURING_WRITER_PHASE
    
}
