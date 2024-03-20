package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.crypto.utils.IoUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

class JsonLinesFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final ObjectMapper objectMapper;

    private LineNumberReader lineNumberReader;
    private InternalRow nextRowToReturn;

    JsonLinesFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public boolean next() throws IOException {
        if (lineNumberReader == null) {
            lineNumberReader = new LineNumberReader(new InputStreamReader(fileContext.open(filePartition)));
        }
        String line = lineNumberReader.readLine();
        if (line == null) {
            return false;
        }
        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(this.filePartition + "-" + lineNumberReader.getLineNumber());
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
        IoUtils.closeQuietly(lineNumberReader);
    }
}
