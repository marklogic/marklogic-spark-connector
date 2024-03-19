package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * This uses the same technique as QueryBatcher in getting back an ordered list of URIs without having to paginate.
 * It does involve 2x calls for each batch - one to get the URIs, and then one to get the documents for those URIs.
 * Will performance test this later to determine if just using documentManager.search with pagination is generally
 * faster. That's just 1 call, but it incurs the cost of finding page N.
 */
class ForestReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ForestReader.class);

    private final UriBatcher uriBatcher;
    private final GenericDocumentManager documentManager;
    private final StructuredQueryBuilder queryBuilder;
    private final Set<DocumentManager.Metadata> requestedMetadata;
    private final boolean contentWasRequested;
    private final Integer limit;

    // Only used for logging.
    private final ForestPartition forestPartition;
    private long startTime;

    private DocumentPage currentDocumentPage;

    // Used for logging and for ensuring a non-null limit is not exceeded.
    private int docCount;

    ForestReader(ForestPartition forestPartition, DocumentContext context) {
        this.forestPartition = forestPartition;
        this.limit = context.getLimit();

        DatabaseClient client = context.isDirectConnection() ?
            context.connectToMarkLogic(forestPartition.getHost()) :
            context.connectToMarkLogic();

        if (logger.isDebugEnabled()) {
            logger.debug("Will read from host {} for partition: {}", client.getHost(), forestPartition);
        }

        SearchQueryDefinition query = context.buildSearchQuery(client);
        boolean filtered = false;
        if (context.hasOption(Options.READ_DOCUMENTS_FILTERED)) {
            filtered = Boolean.parseBoolean(context.getProperties().get(Options.READ_DOCUMENTS_FILTERED));
        }
        this.uriBatcher = new UriBatcher(client, query, forestPartition, context.getBatchSize(), filtered);

        this.documentManager = client.newDocumentManager();
        this.documentManager.setReadTransform(query.getResponseTransform());
        this.contentWasRequested = context.contentWasRequested();
        this.requestedMetadata = context.getRequestedMetadata();
        this.documentManager.setMetadataCategories(this.requestedMetadata);
        this.queryBuilder = client.newQueryManager().newStructuredQueryBuilder();
    }

    @Override
    public boolean next() {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        if (limit != null && docCount >= limit) {
            // No logging here as this block may never be hit, depending on whether Spark first detects that the limit
            // has been reached.
            return false;
        }

        if (currentDocumentPage == null || !currentDocumentPage.hasNext()) {
            closeCurrentDocumentPage();
            List<String> uris = getNextBatchOfUris();
            if (uris.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    long duration = System.currentTimeMillis() - startTime;
                    logger.debug("Read {} documents from partition {} in {}ms", docCount, forestPartition, duration);
                }
                return false;
            }
            this.currentDocumentPage = readPage(uris);
        }

        return currentDocumentPage.hasNext();
    }

    @Override
    public InternalRow get() {
        DocumentRecord document = this.currentDocumentPage.next();

        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(document.getUri());
        if (this.contentWasRequested) {
            row[1] = ByteArray.concat(document.getContent(new BytesHandle()).get());
            final String format = document.getFormat() != null ? document.getFormat().toString() : Format.UNKNOWN.toString();
            row[2] = UTF8String.fromString(format);
        }

        if (!requestedMetadata.isEmpty()) {
            DocumentMetadataHandle metadata = document.getMetadata(new DocumentMetadataHandle());
            populateMetadataColumns(row, metadata);
        }

        docCount++;
        return new GenericInternalRow(row);
    }

    @Override
    public void close() {
        closeCurrentDocumentPage();
    }
    
    private List<String> getNextBatchOfUris() {
        long start = System.currentTimeMillis();
        List<String> uris = uriBatcher.nextBatchOfUris();
        if (logger.isTraceEnabled()) {
            logger.trace("Retrieved {} URIs in {}ms from partition {}", uris.size(),
                (System.currentTimeMillis() - start), this.forestPartition);
        }
        return uris;
    }

    private DocumentPage readPage(List<String> uris) {
        long start = System.currentTimeMillis();
        String[] uriArray = uris.toArray(new String[]{});

        QueryDefinition queryDefinition = this.queryBuilder.document(uriArray);
        this.documentManager.setPageLength(uriArray.length);

        // Must do a search so that a POST is sent instead of a GET. A GET can fail with a Request-URI error if too
        // many URIs are included in the querystring. However, content is always retrieved with a search, so there's
        // some inefficiency if the caller only wants metadata and no content.
        DocumentPage page = this.documentManager.search(queryDefinition, 0);
        if (logger.isTraceEnabled()) {
            logger.trace("Retrieved page of documents in {}ms from partition {}", (System.currentTimeMillis() - start), this.forestPartition);
        }
        return page;
    }

    private void populateMetadataColumns(Object[] row, DocumentMetadataHandle metadata) {
        if (requestedMetadataHas(DocumentManager.Metadata.COLLECTIONS)) {
            DocumentRowSchema.populateCollectionsColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.PERMISSIONS)) {
            DocumentRowSchema.populatePermissionsColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.QUALITY)) {
            DocumentRowSchema.populateQualityColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.PROPERTIES)) {
            DocumentRowSchema.populatePropertiesColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.METADATAVALUES)) {
            DocumentRowSchema.populateMetadataValuesColumn(row, metadata);
        }
    }

    private boolean requestedMetadataHas(DocumentManager.Metadata metadata) {
        return requestedMetadata.contains(metadata) || requestedMetadata.contains(DocumentManager.Metadata.ALL);
    }

    private void closeCurrentDocumentPage() {
        if (currentDocumentPage != null) {
            currentDocumentPage.close();
            currentDocumentPage = null;
        }
    }
}
