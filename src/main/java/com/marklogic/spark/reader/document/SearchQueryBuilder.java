package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.SearchQueryDefinition;

/**
 * Potentially reusable class for the Java Client that handles constructing a query based on a common
 * set of user-defined inputs.
 */
class SearchQueryBuilder {

    public enum QueryType {
        STRING,
        STRUCTURED,
        SERIALIZED_CTS,
        COMBINED;
    }

    private String query;
    private QueryType queryType;
    private String[] collections;

    SearchQueryDefinition buildQuery(DatabaseClient client) {
        final QueryManager queryManager = client.newQueryManager();
        if (query != null) {
            QueryDefinition queryDefinition;
            if (QueryType.STRUCTURED.equals(queryType)) {
                queryDefinition = queryManager.newRawStructuredQueryDefinition(new StringHandle(query));
            } else if (QueryType.SERIALIZED_CTS.equals(queryType)) {
                queryDefinition = queryManager.newRawCtsQueryDefinition(new StringHandle(query));
            } else if (QueryType.COMBINED.equals(queryType)) {
                queryDefinition = queryManager.newRawCombinedQueryDefinition(new StringHandle(query));
            } else {
                queryDefinition = client.newQueryManager().newStringDefinition().withCriteria(this.query);
            }
            if (collections != null) {
                queryDefinition.setCollections(this.collections);
            }
            return queryDefinition;
        }
        return queryManager.newStructuredQueryBuilder().collection(collections);
    }

    public SearchQueryBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public SearchQueryBuilder withQueryType(String queryType) {
        if (queryType != null) {
            this.queryType = QueryType.valueOf(queryType.toUpperCase());
        }
        return this;
    }

    public SearchQueryBuilder withCollections(String value) {
        if (value != null) {
            this.collections = value.split(",");
        }
        return this;
    }
}
