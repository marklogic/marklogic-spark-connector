package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.impl.QueryManagerImpl;
import com.marklogic.client.impl.UrisHandle;
import com.marklogic.client.query.SearchQueryDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Retrieves batches of URIs in the same manner as {@code QueryBatcher} in the Java Client, without resorting to
 * pagination. May eventually want this to be in the Java Client for reuse, as the need to get back a distinct set of
 * URIs without resorting to pagination is common.
 */
class UriBatcher {

    private final DatabaseClient client;
    private final QueryManagerImpl queryManager;
    private final SearchQueryDefinition query;
    private final ForestPartition partition;
    private final int pageLength;
    private final boolean filtered;

    // These change as batches of URIs are retrieved.
    private String lastUri;
    private long offsetStart = 1;


    UriBatcher(DatabaseClient client, SearchQueryDefinition query, ForestPartition partition, int pageLength, boolean filtered) {
        this.client = client;
        this.queryManager = (QueryManagerImpl) this.client.newQueryManager();
        this.queryManager.setPageLength(pageLength);
        this.query = query;
        this.partition = partition;
        this.offsetStart = this.partition.getOffsetStart();
        this.pageLength = pageLength;
        this.filtered = filtered;
    }

    /**
     * @return the next batch of matching URIs, where "next" = URIs after "lastUri", if it's not null. The
     * {@code queryManager.uris} method will order the URIs so that we are guaranteed to get URIs we have not seen yet
     * without resorting to pagination.
     */
    List<String> nextBatchOfUris() {
        if (partition.getOffsetEnd() != null && this.offsetStart > partition.getOffsetEnd()) {
            return new ArrayList<>();
        }

        UrisHandle urisHandle = new UrisHandle();
        urisHandle.setPointInTimeQueryTimestamp(partition.getServerTimestamp());

        // If we have an offsetEnd, the page length is adjusted to ensure we do not go past offsetEnd.
        if (partition.getOffsetEnd() != null && (this.offsetStart + this.pageLength > partition.getOffsetEnd())) {
            // e.g. 9001 to 10000; need a page length of 1000, so 1 is added.
            int finalPageLength = (int) (partition.getOffsetEnd() - this.offsetStart) + 1;
            this.queryManager.setPageLength(finalPageLength);
        }

        try (UrisHandle results = queryManager.uris("POST", query, filtered, urisHandle, offsetStart, lastUri, partition.getForestName())) {
            List<String> uris = new ArrayList<>();
            results.iterator().forEachRemaining(uris::add);
            if (!uris.isEmpty()) {
                this.lastUri = uris.get(uris.size() - 1);
            }
            this.offsetStart = this.offsetStart + uris.size();
            return uris;
        } catch (ResourceNotFoundException ex) {
            // QueryBatcherImpl notes that this is how internal/uris informs us that there are no results left.
            return new ArrayList<>();
        }
    }
}
