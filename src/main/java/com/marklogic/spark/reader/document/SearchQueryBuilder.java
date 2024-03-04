package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;
import com.marklogic.spark.Util;

/**
 * Potentially reusable class for the Java Client that handles constructing a query based on a common
 * set of user-defined inputs.
 */
public class SearchQueryBuilder {

    private String stringQuery;
    private String query;
    private String[] collections;
    private String directory;
    private String optionsName;
    private String transformName;
    private String transformParams;
    private String transformParamsDelimiter;
    private String[] uris;

    SearchQueryDefinition buildQuery(DatabaseClient client) {
        QueryDefinition queryDefinition = buildQueryDefinition(client);
        applyCommonQueryConfig(queryDefinition);
        return queryDefinition;
    }

    /**
     * Corresponds to the "q" request parameter as defined by https://docs.marklogic.com/REST/POST/v1/search .
     *
     * @param stringQuery
     * @return
     */
    public SearchQueryBuilder withStringQuery(String stringQuery) {
        this.stringQuery = stringQuery;
        return this;
    }

    /**
     * Corresponds to the request body as defined by https://docs.marklogic.com/REST/POST/v1/search . Can be either
     * a structured query, a serialized CTS query, or a combined query. Can be defined as either JSON or XML.
     *
     * @param query
     * @return
     */
    public SearchQueryBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public SearchQueryBuilder withCollections(String value) {
        if (value != null) {
            this.collections = value.split(",");
        }
        return this;
    }

    public SearchQueryBuilder withDirectory(String directory) {
        this.directory = directory;
        return this;
    }

    public SearchQueryBuilder withOptionsName(String optionsName) {
        this.optionsName = optionsName;
        return this;
    }

    public SearchQueryBuilder withTransformName(String transformName) {
        this.transformName = transformName;
        return this;
    }

    public SearchQueryBuilder withTransformParams(String transformParams) {
        this.transformParams = transformParams;
        return this;
    }

    public SearchQueryBuilder withTransformParamsDelimiter(String delimiter) {
        this.transformParamsDelimiter = delimiter;
        return this;
    }

    public SearchQueryBuilder withUris(String... uris) {
        this.uris = uris;
        return this;
    }

    private QueryDefinition buildQueryDefinition(DatabaseClient client) {
        final QueryManager queryManager = client.newQueryManager();

        if (uris != null && uris.length > 0) {
            StructuredQueryDefinition urisQuery = queryManager.newStructuredQueryBuilder().document(this.uris);
            if (stringQuery != null && stringQuery.length() > 0) {
                urisQuery.withCriteria(stringQuery);
            }
            if (this.query != null) {
                Util.MAIN_LOGGER.warn("Ignoring query since a list of URIs was provided; query: {}", this.query);
            }
            return urisQuery;
        }

        if (query != null) {
            StringHandle queryHandle = new StringHandle(query);
            // v1/search assumes XML by default, so only need to set to JSON if the query is JSON.
            if (queryIsJSON()) {
                queryHandle.withFormat(Format.JSON);
            }
            // The Java Client misleadingly suggests a distinction amongst the 3 complex queries - structured,
            // serialized CTS, and combined - but the REST API does not. Thus, a RawStructuredQueryDefinition will work
            // for any of the 3 query types.
            RawStructuredQueryDefinition queryDefinition = queryManager.newRawStructuredQueryDefinition(queryHandle);
            if (stringQuery != null && stringQuery.length() > 0) {
                queryDefinition.setCriteria(stringQuery);
            }
            return queryDefinition;
        }

        StringQueryDefinition queryDefinition = queryManager.newStringDefinition();
        if (this.stringQuery != null && stringQuery.length() > 0) {
            queryDefinition.setCriteria(this.stringQuery);
        }

        return queryDefinition;
    }

    private boolean queryIsJSON() {
        return query != null && query.trim().startsWith("{");
    }

    private void applyCommonQueryConfig(QueryDefinition queryDefinition) {
        if (optionsName != null && optionsName.trim().length() > 0) {
            queryDefinition.setOptionsName(optionsName);
        }
        if (collections != null && collections.length > 0) {
            queryDefinition.setCollections(this.collections);
        }
        if (directory != null && directory.trim().length() > 0) {
            queryDefinition.setDirectory(directory);
        }
        if (transformName != null && transformName.trim().length() > 0) {
            queryDefinition.setResponseTransform(buildServerTransform());
        }
    }

    private ServerTransform buildServerTransform() {
        ServerTransform transform = new ServerTransform(transformName);
        if (transformParams != null && transformParams.trim().length() > 0) {
            String delimiter = transformParamsDelimiter != null && transformParamsDelimiter.trim().length() > 0 ? transformParamsDelimiter : ",";
            String[] params = transformParams.split(delimiter);
            if (params.length % 2 != 0) {
                throw new IllegalArgumentException("Transform params must have an equal number of parameter names and values: " + transformParams);
            }
            for (int i = 0; i < params.length; i += 2) {
                String name = params[i];
                String value = params[i + 1];
                transform.addParameter(name, value);
            }
        }
        return transform;
    }
}
