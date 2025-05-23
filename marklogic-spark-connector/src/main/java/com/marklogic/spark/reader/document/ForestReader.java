/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.ReadProgressLogger;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.ServerEvaluationCallFactory;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
    private final GenericDocumentManager documentManager;
    private final StructuredQueryBuilder queryBuilder;
    private final Set<DocumentManager.Metadata> requestedMetadata;
    private final boolean contentWasRequested;
    private final Integer limit;
    private final boolean isStreamingFiles;
    private final Optional<ServerEvaluationCallFactory> callFactory;
    private final ObjectMapper objectMapper;

    // Only used for logging.
    private final ForestPartition forestPartition;
    private long startTime;

    private DocumentPage currentDocumentPage;

    // Used for logging and for ensuring a non-null limit is not exceeded.
    private int docCount;

    ForestReader(ForestPartition forestPartition, DocumentContext context) {
        this.forestPartition = forestPartition;
        this.limit = context.getLimit();
        this.isStreamingFiles = context.isStreamingFiles();

        this.databaseClient = context.isDirectConnection() ?
            context.connectToMarkLogic(forestPartition.getHost()) :
            context.connectToMarkLogic();

        final boolean filtered = context.getBooleanOption(Options.READ_DOCUMENTS_FILTERED, false);
        final boolean consistentSnapshot = context.isConsistentSnapshot();

        if (logger.isDebugEnabled()) {
            logger.debug("Will read from host {} for partition: {}; filtered: {}; consistent snapshot: {}",
                databaseClient.getHost(), forestPartition, filtered, consistentSnapshot);
        }

        SearchQueryDefinition query = context.buildSearchQuery(databaseClient);
        this.uriBatcher = new UriBatcher(databaseClient, query, forestPartition, context.getBatchSize(), filtered, consistentSnapshot);

        this.documentManager = databaseClient.newDocumentManager();
        this.documentManager.setReadTransform(query.getResponseTransform());
        this.contentWasRequested = context.contentWasRequested();
        this.requestedMetadata = ContextSupport.getRequestedMetadata(context);
        this.documentManager.setMetadataCategories(this.requestedMetadata);
        this.queryBuilder = databaseClient.newQueryManager().newStructuredQueryBuilder();

        this.callFactory = new ServerEvaluationCallFactory.Builder()
            .withInvokeOptionName(Options.READ_SECONDARY_URIS_INVOKE)
            .withJavascriptOptionName(Options.READ_SECONDARY_URIS_JAVASCRIPT)
            .withXqueryOptionName(Options.READ_SECONDARY_URIS_XQUERY)
            .withJavascriptFileOptionName(Options.READ_SECONDARY_URIS_JAVASCRIPT_FILE)
            .withXqueryFileOptionName(Options.READ_SECONDARY_URIS_XQUERY_FILE)
            .withVarsPrefix(Options.READ_SECONDARY_URIS_VARS_PREFIX)
            .build(context);

        this.objectMapper = this.callFactory.isPresent() ? new ObjectMapper() : null;
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

            if (this.callFactory.isPresent()) {
                addUrisToBatchViaSecondaryQuery(uris);
            }

            // When streaming, we don't want to retrieve the documents yet - they'll be retrieved in the writer phase.
            this.currentDocumentPage = this.isStreamingFiles ? new UrisPage(uris.iterator()) : readPage(uris);
        }

        return currentDocumentPage.hasNext();
    }

    @Override
    public InternalRow get() {
        DocumentRecord document = this.currentDocumentPage.next();
        DocumentRowBuilder builder = new DocumentRowBuilder(requestedMetadata).withUri(document.getUri());
        if (this.contentWasRequested) {
            BytesHandle content = document.getContent(new BytesHandle());
            Objects.requireNonNull(content);
            builder.withContent(content.get());
            builder.withFormat(document.getFormat() != null ? document.getFormat().toString() : Format.UNKNOWN.toString());
        }
        if (!requestedMetadata.isEmpty()) {
            builder.withMetadata(document.getMetadata(new DocumentMetadataHandle()));
        }
        docCount++;
        return builder.buildRow();
    }

    @Override
    public void close() {
        closeCurrentDocumentPage();
        if (databaseClient != null) {
            databaseClient.release();
        }
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
        ReadProgressLogger.logProgressIfNecessary(page.getPageSize());
        return page;
    }

    private void addUrisToBatchViaSecondaryQuery(List<String> uris) {
        ServerEvaluationCall call = this.callFactory.get().newCall(databaseClient);
        ArrayNode urisArray = this.objectMapper.createArrayNode();
        for (String uri : uris) {
            urisArray.add(uri);
        }
        call.addVariable("URIs", new JacksonHandle(urisArray).withFormat(Format.JSON));
        int countOfAdditionalUris = 0;
        try (EvalResultIterator iterator = call.eval()) {
            while (iterator.hasNext()) {
                String uri = iterator.next().getAs(String.class);
                countOfAdditionalUris++;
                uris.add(uri);
            }
        }
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Added {} URIs; new batch size: {}", countOfAdditionalUris, uris.size());
        }
    }

    private void closeCurrentDocumentPage() {
        if (currentDocumentPage != null) {
            IOUtils.closeQuietly(currentDocumentPage);
            currentDocumentPage = null;
        }
    }
}
