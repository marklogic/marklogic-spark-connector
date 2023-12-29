package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This uses the same technique as QueryBatcher in getting back an ordered list of URIs without having to paginate.
 * It does involve 2x calls for each batch - one to get the URIs, and then one to get the documents for those URIs.
 * Will performance test this later to determine if just using documentManager.search with pagination is generally
 * faster. That's just 1 call, but it incurs the cost of finding page N.
 */
class ForestReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ForestReader.class);
    private final DatabaseClient databaseClient;
    private final UriBatcher uriBatcher;

    // Only used for logging.
    private final String forestName;

    private DocumentPage currentDocumentPage;

    ForestReader(ForestPartition forestPartition, DocumentContext documentContext) {
        this.forestName = forestPartition.getForestName();
        if (logger.isInfoEnabled()) {
            logger.info("Will read from forest: {}", this.forestName);
        }

        this.databaseClient = documentContext.connectToMarkLogic();
        String[] collections = documentContext.getProperties().get(Options.READ_DOCUMENTS_COLLECTIONS).split(",");
        StructuredQueryDefinition query = databaseClient.newQueryManager()
            .newStructuredQueryBuilder()
            .collection(collections);
        this.uriBatcher = new UriBatcher(databaseClient, query, forestPartition.getForestName());
    }

    @Override
    public boolean next() {
        if (currentDocumentPage == null || !currentDocumentPage.hasNext()) {
            List<String> uris = uriBatcher.nextBatchOfUris();
            if (uris.isEmpty()) {
                return false;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Read {} URIs from forest {}", uris.size(), this.forestName);
            }
            // Copied from ExportListener. Will add transform support later.
            this.currentDocumentPage = this.databaseClient.newDocumentManager()
                .read((ServerTransform) null, uris.toArray(new String[]{}));
        }
        return currentDocumentPage.hasNext();
    }

    @Override
    public InternalRow get() {
        DocumentRecord record = this.currentDocumentPage.next();
        String format = record.getFormat() != null ? record.getFormat().toString() : Format.UNKNOWN.toString();
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(record.getUri()),
            ByteArray.concat(record.getContent(new BytesHandle()).get()),
            UTF8String.fromString(format)
        });
    }

    @Override
    public void close() {
    }
}
