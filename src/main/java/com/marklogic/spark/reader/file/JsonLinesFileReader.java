/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedReader;
import java.util.Iterator;

class JsonLinesFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private BufferedReader bufferedReader;
    private Iterator<String> bufferedLines;

    private InternalRow nextRowToReturn;
    private String currentFilePath;
    private int lineCounter;
    private int filePathIndex;

    JsonLinesFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        if (bufferedLines != null && bufferedLines.hasNext()) {
            this.nextRowToReturn = createRowFromNextJsonLine();
            return true;
        }

        if (bufferedReader != null) {
            IOUtils.closeQuietly(bufferedReader);
        }

        if (filePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        openNextFile();
        return next();
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(bufferedReader);
    }

    private void openNextFile() {
        this.currentFilePath = filePartition.getPaths().get(filePathIndex);
        this.lineCounter = 1;
        this.filePathIndex++;
        // To mimic the behavior of the Spark JSON data source, this will guess if the file is gzipped based on its
        // file extension. This allows for .gz/.gzip files to be supported without the user having to specify the
        // compression option, which is the same behavior as Spark JSON provides.
        this.bufferedReader = fileContext.openFileReader(currentFilePath, true);
        this.bufferedLines = bufferedReader.lines().iterator();
    }

    private InternalRow createRowFromNextJsonLine() {
        String line = bufferedLines.next();
        String uri = String.format("%s-%d.json", UTF8String.fromString(currentFilePath), lineCounter);
        lineCounter++;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri),
            ByteArray.concat(line.getBytes()),
            UTF8String.fromString("JSON"),
            null, null, null, null, null
        });
    }
}
