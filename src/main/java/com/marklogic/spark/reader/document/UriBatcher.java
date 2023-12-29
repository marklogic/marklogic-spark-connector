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
    private final SearchQueryDefinition query;
    private final boolean filtered;
    private final String forestName;
    private final int pageLength;

    private long serverTimestamp = -1;
    private String lastUri;
    private long start = 1;

    UriBatcher(DatabaseClient client, SearchQueryDefinition query, String forestName) {
        this(client, query, forestName, 500, false);
    }

    UriBatcher(DatabaseClient client, SearchQueryDefinition query, String forestName, int pageLength, boolean filtered) {
        this.client = client;
        this.query = query;
        this.filtered = filtered;
        this.forestName = forestName;
        this.pageLength = pageLength;
    }

    /**
     * @return the next batch of matching URIs, where "next" = URIs after "lastUri", if it's not null. The
     * {@code queryManager.uris} method will order the URIs so that we are guaranteed to get URIs we have not seen yet
     * without resorting to pagination.
     */
    List<String> nextBatchOfUris() {
        QueryManagerImpl queryManager = (QueryManagerImpl) client.newQueryManager();
        queryManager.setPageLength(this.pageLength);
        UrisHandle urisHandle = new UrisHandle();
        if (this.serverTimestamp > -1) {
            urisHandle.setPointInTimeQueryTimestamp(this.serverTimestamp);
        }

        try (UrisHandle results = queryManager.uris("POST", query, filtered, urisHandle, start, lastUri, forestName)) {
            if (serverTimestamp == -1) {
                this.serverTimestamp = results.getServerTimestamp();
            }
            List<String> uris = new ArrayList<>();
            results.iterator().forEachRemaining(uri -> uris.add(uri));
            if (!uris.isEmpty()) {
                this.lastUri = uris.get(uris.size() - 1);
            }
            this.start = this.start + uris.size();
            return uris;
        } catch (ResourceNotFoundException ex) {
            // QueryBatcherImpl notes that this is how internal/uris informs us that there are no results left.
            return new ArrayList<>();
        }
    }
}
