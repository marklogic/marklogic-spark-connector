package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.marklogic.client.datamovement.JacksonCSVSplitter;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

class DelimitedTextFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InputStream inputStream;
    private Iterator<DocumentWriteOperation> documentIterator;
    private InternalRow nextRowToReturn;

    DelimitedTextFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() throws IOException {
        if (documentIterator == null) {
            this.inputStream = fileContext.open(filePartition);
            JacksonCSVSplitter splitter = new JacksonCSVSplitter();
            // TODO We can support the "-data_type" feature in MLCP by translating that into a CsvSchema.
            // TODO We'll need a delimiter option.
//            splitter.withCsvSchema(CsvSchema.emptySchema().withColumnSeparator(';').withHeader());
            try {
                documentIterator = splitter.splitWriteOperations(this.inputStream).iterator();
            } catch (Exception e) {
                // Why is this an exception???
                throw new ConnectorException("Make this nicer later.", e);
            }
        }
        if (!documentIterator.hasNext()) {
            return false;
        }
        DocumentWriteOperation writeOp = documentIterator.next();
        Object[] row = new Object[8];
        // The default URI should be fine, as URI template will soon be usable for it.
        row[0] = UTF8String.fromString(writeOp.getUri());
        JacksonHandle content = (JacksonHandle) writeOp.getContent();
        row[1] = ByteArray.concat(content.toBuffer());
        row[2] = UTF8String.fromString("json");
        this.nextRowToReturn = new GenericInternalRow(row);
        return true;
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(inputStream);
    }
}
