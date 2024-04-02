package com.marklogic.spark.reader.file;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Because this
 */
class JsonLinesFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private BufferedReader reader;
    private int lineNumber;
    private InternalRow nextRowToReturn;

    JsonLinesFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() throws IOException {
        if (reader == null) {
            reader = new BufferedReader(new InputStreamReader(fileContext.open(filePartition)));
        }
        String line = reader.readLine();
        if (line == null) {
            return false;
        }
        lineNumber++;
        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(String.format("%s-%d.json", this.filePartition, lineNumber));
        row[1] = ByteArray.concat(line.getBytes());
        row[2] = UTF8String.fromString("json");
        this.nextRowToReturn = new GenericInternalRow(row);
        return true;
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(reader);
    }
}
