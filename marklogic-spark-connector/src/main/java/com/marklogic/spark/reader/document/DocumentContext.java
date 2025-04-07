/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

class DocumentContext extends ContextSupport {

    private Integer limit;
    private final StructType schema;

    DocumentContext(CaseInsensitiveStringMap options, StructType schema) {
        super(options.asCaseSensitiveMap());
        this.schema = schema;
    }

    boolean contentWasRequested() {
        if (isStreamingFiles()) {
            return false;
        }
        if (!hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            return true;
        }
        for (String value : getProperties().get(Options.READ_DOCUMENTS_CATEGORIES).split(",")) {
            if ("content".equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    SearchQueryDefinition buildSearchQuery(DatabaseClient client) {
        final Map<String, String> props = getProperties();
        // REST API allows commas in URIs, but not newlines, so that's safe to use as a delimiter.
        String[] uris = null;
        if (hasOption(Options.READ_DOCUMENTS_URIS)) {
            uris = getStringOption(Options.READ_DOCUMENTS_URIS).split("\n");
        }
        return new SearchQueryBuilder()
            .withStringQuery(props.get(Options.READ_DOCUMENTS_STRING_QUERY))
            .withQuery(props.get(Options.READ_DOCUMENTS_QUERY))
            .withCollections(props.get(Options.READ_DOCUMENTS_COLLECTIONS))
            .withDirectory(props.get(Options.READ_DOCUMENTS_DIRECTORY))
            .withOptionsName(props.get(Options.READ_DOCUMENTS_OPTIONS))
            .withTransformName(props.get(Options.READ_DOCUMENTS_TRANSFORM))
            .withTransformParams(props.get(Options.READ_DOCUMENTS_TRANSFORM_PARAMS))
            .withTransformParamsDelimiter(props.get(Options.READ_DOCUMENTS_TRANSFORM_PARAMS_DELIMITER))
            .withUris(uris)
            .buildQuery(client);
    }

    SearchQueryDefinition buildTriplesSearchQuery(DatabaseClient client) {
        final Map<String, String> props = getProperties();
        String[] uris = null;
        if (hasOption(Options.READ_TRIPLES_URIS)) {
            uris = getStringOption(Options.READ_TRIPLES_URIS).split("\n");
        }
        return new SearchQueryBuilder()
            .withStringQuery(props.get(Options.READ_TRIPLES_STRING_QUERY))
            .withQuery(props.get(Options.READ_TRIPLES_QUERY))
            .withCollections(combineCollectionsAndGraphs())
            .withDirectory(props.get(Options.READ_TRIPLES_DIRECTORY))
            .withOptionsName(props.get(Options.READ_TRIPLES_OPTIONS))
            .withUris(uris)
            .buildQuery(client);
    }

    private String combineCollectionsAndGraphs() {
        String graphs = getProperties().get(Options.READ_TRIPLES_GRAPHS);
        String collections = getProperties().get(Options.READ_TRIPLES_COLLECTIONS);
        if (graphs != null && !graphs.trim().isEmpty()) {
            if (collections == null || collections.trim().isEmpty()) {
                collections = graphs;
            } else {
                collections += "," + graphs;
            }
        }
        return collections;
    }

    int getBatchSize() {
        // Testing has shown that at least for smaller documents, 100 or 200 can be significantly slower than something
        // like 1000 or even 10000. 500 is thus used as a default that should still be reasonably performant for larger
        // documents.
        int defaultBatchSize = 500;
        return getIntOption(Options.READ_BATCH_SIZE, defaultBatchSize, 1);
    }

    int getPartitionsPerForest() {
        int defaultPartitionsPerForest = 4;
        return getIntOption(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, defaultPartitionsPerForest, 1);
    }

    boolean isConsistentSnapshot() {
        // Starting in 2.2.0 and through 2.4.2, the default is a consistent snapshot. We may change this later.
        return getBooleanOption(Options.READ_SNAPSHOT, true);
    }

    void setLimit(Integer limit) {
        this.limit = limit;
    }

    Integer getLimit() {
        return limit;
    }

    StructType getSchema() {
        return schema;
    }
}
