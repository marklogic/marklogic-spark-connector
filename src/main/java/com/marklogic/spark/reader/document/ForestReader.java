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
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        this.uriBatcher = new UriBatcher(client, query, forestPartition, context.getBatchSize(), false);

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
                // TBD on whether this should be info/debug.
                if (Util.MAIN_LOGGER.isInfoEnabled()) {
                    long duration = System.currentTimeMillis() - startTime;
                    Util.MAIN_LOGGER.info("Read {} documents from partition {} in {}ms", docCount, forestPartition, duration);
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
            populateCollectionsColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.PERMISSIONS)) {
            populatePermissionsColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.QUALITY)) {
            row[5] = metadata.getQuality();
        }
        if (requestedMetadataHas(DocumentManager.Metadata.PROPERTIES)) {
            populatePropertiesColumn(row, metadata);
        }
        if (requestedMetadataHas(DocumentManager.Metadata.METADATAVALUES)) {
            populateMetadataValuesColumn(row, metadata);
        }
    }

    private void populateCollectionsColumn(Object[] row, DocumentMetadataHandle metadata) {
        UTF8String[] collections = new UTF8String[metadata.getCollections().size()];
        Iterator<String> iterator = metadata.getCollections().iterator();
        for (int i = 0; i < collections.length; i++) {
            collections[i] = UTF8String.fromString(iterator.next());
        }
        row[3] = ArrayData.toArrayData(collections);
    }

    private void populatePermissionsColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
        UTF8String[] roles = new UTF8String[perms.size()];
        Object[] capabilityArrays = new Object[perms.size()];
        int i = 0;
        for (Map.Entry<String, Set<DocumentMetadataHandle.Capability>> entry : perms.entrySet()) {
            roles[i] = UTF8String.fromString(entry.getKey());
            UTF8String[] capabilities = new UTF8String[entry.getValue().size()];
            int j = 0;
            Iterator<DocumentMetadataHandle.Capability> iterator = entry.getValue().iterator();
            while (iterator.hasNext()) {
                capabilities[j++] = UTF8String.fromString(iterator.next().name());
            }
            capabilityArrays[i++] = ArrayData.toArrayData(capabilities);
        }
        row[4] = ArrayBasedMapData.apply(roles, capabilityArrays);
    }

    private void populatePropertiesColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentProperties props = metadata.getProperties();
        UTF8String[] keys = new UTF8String[props.size()];
        UTF8String[] values = new UTF8String[props.size()];
        int index = 0;
        for (QName key : props.keySet()) {
            keys[index] = UTF8String.fromString(key.toString());
            values[index++] = UTF8String.fromString(props.get(key, String.class));
        }
        row[6] = ArrayBasedMapData.apply(keys, values);
    }

    private void populateMetadataValuesColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentMetadataValues metadataValues = metadata.getMetadataValues();
        UTF8String[] keys = new UTF8String[metadataValues.size()];
        UTF8String[] values = new UTF8String[metadataValues.size()];
        int index = 0;
        for (Map.Entry<String, String> entry : metadataValues.entrySet()) {
            keys[index] = UTF8String.fromString(entry.getKey());
            values[index++] = UTF8String.fromString(entry.getValue());
        }
        row[7] = ArrayBasedMapData.apply(keys, values);
    }

    private boolean requestedMetadataHas(DocumentManager.Metadata metadata) {
        return requestedMetadata.contains(metadata) || requestedMetadata.contains(DocumentManager.Metadata.ALL);
    }

    @Override
    public void close() {
        closeCurrentDocumentPage();
    }

    private void closeCurrentDocumentPage() {
        if (currentDocumentPage != null) {
            currentDocumentPage.close();
            currentDocumentPage = null;
        }
    }
}
