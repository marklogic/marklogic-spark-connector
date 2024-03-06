package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Functions the same as reading an aggregate XML file - it just needs to wrap the input stream in a
 * {@code GZIPInputStream} first.
 */
class GZIPAggregateXMLFileReader extends AggregateXMLFileReader {

    GZIPAggregateXMLFileReader(FilePartition filePartition, FileContext fileContext) {
        super(filePartition, fileContext);
    }

    @Override
    protected InputStream makeInputStream(FilePartition filePartition, FileContext fileContext) {
        InputStream inputStream = fileContext.open(filePartition);
        try {
            return new GZIPInputStream(inputStream);
        } catch (IOException ex) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePartition.getPath(), ex.getMessage()));
        }
    }
}
