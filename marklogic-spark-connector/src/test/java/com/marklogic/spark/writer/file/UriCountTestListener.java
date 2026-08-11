/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.api.FileWriteListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test-only listener for capturing URI count callbacks fired by a file writer. Tests can access the static fields
 * to verify counts, since the listener is instantiated reflectively by the writer being tested.
 */
public class UriCountTestListener implements FileWriteListener {

    public static final List<Long> writtenUriCounts = new ArrayList<>();
    public static Map<String, String> params;

    public UriCountTestListener(Map<String, String> params) {
        UriCountTestListener.params = params;
    }

    @Override
    public void onUrisProcessed(long totalUriCount) {
        writtenUriCounts.add(totalUriCount);
    }

    public static void reset() {
        writtenUriCounts.clear();
        params = null;
    }
}
