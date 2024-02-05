package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.SearchQueryDefinition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Responsible for reading all the documents across a range of pages, as defined by the given
 * {@code PageRangePartition}. Will make N calls to MarkLogic based on the number of documents across the range of
 * pages and the user-defined batch size, which equates to a page length for the v1/search endpoint.
 */
class PageRangeReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(PageRangeReader.class);

    private final PageRangePartition partition;
    private final GenericDocumentManager documentManager;
    private final SearchQueryDefinition queryDefinition;
    private final int batchSize;

    // Defines the offset in the overall set of search results for each call to v1/search.
    private long offset;
    private DocumentPage currentDocumentPage;

    // Used for logging/debugging.
    private long startTime;
    private int docCount;

    public PageRangeReader(PageRangePartition partition, DocumentContext context) {
        this.partition = partition;
        if (logger.isDebugEnabled()) {
            logger.debug("Will process bucket: {}", this.partition);
        }

        this.offset = partition.getOffsetStart();
        this.batchSize = context.getBatchSize();

        DatabaseClient client = context.connectToMarkLogic();
        this.documentManager = client.newDocumentManager();
        this.queryDefinition = context.buildSearchQuery(client);
        // In the case of only a single request being needed, page length is the size of the partition to avoid
        // returning more documents than are needed.
        long pageLength = partition.getLength() < this.batchSize ? partition.getLength() : this.batchSize;
        this.documentManager.setPageLength(pageLength);

        // Is it okay to initialize this in the constructor?
        this.currentDocumentPage = this.documentManager.search(this.queryDefinition, offset, this.partition.getServerTimestamp());
    }

    @Override
    public boolean next() throws IOException {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        // Make sure we never get more documents than the length of the partition.
        if (this.docCount >= this.partition.getLength()) {
            if (logger.isInfoEnabled()) {
                logger.info("Read {} documents from bucket {} in {}ms", docCount, this.partition, System.currentTimeMillis() - startTime);
            }
            return false;
        }

        if (currentDocumentPage == null || !currentDocumentPage.hasNext()) {
            closeCurrentDocumentPage();
            this.offset += this.batchSize;
            this.currentDocumentPage = this.documentManager.search(this.queryDefinition, offset, this.partition.getServerTimestamp());
            if (this.currentDocumentPage.size() == 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Read {} documents from bucket {} in {}ms", docCount, this.partition, System.currentTimeMillis() - startTime);
                }
                return false;
            }
        }

        return this.currentDocumentPage.hasNext();
    }

    @Override
    public InternalRow get() {
        // TODO Copy all the stuff over for metadata.
        DocumentRecord document = this.currentDocumentPage.next();

        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(document.getUri());
        row[1] = ByteArray.concat(document.getContent(new BytesHandle()).get());
        final String format = document.getFormat() != null ? document.getFormat().toString() : Format.UNKNOWN.toString();
        row[2] = UTF8String.fromString(format);
        row[3] = null;
        row[4] = null;
        row[5] = null;
        row[6] = null;
        row[7] = null;
        docCount++;
        return new GenericInternalRow(row);
    }

    @Override
    public void close() throws IOException {
        closeCurrentDocumentPage();
    }

    private void closeCurrentDocumentPage() {
        if (currentDocumentPage != null) {
            currentDocumentPage.close();
            currentDocumentPage = null;
        }
    }
}
