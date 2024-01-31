package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;

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

    private QueryDefinition buildQueryDefinition(DatabaseClient client) {
        final QueryManager queryManager = client.newQueryManager();
        // The Java Client misleadingly suggests a distinction amongst the 3 complex queries - structured,
        // serialized CTS, and combined - but the REST API does not.
        if (query != null) {
            RawStructuredQueryDefinition queryDefinition = queryManager.newRawStructuredQueryDefinition(new StringHandle(query));
            if (stringQuery != null && stringQuery.length() > 0) {
                queryDefinition.withCriteria(stringQuery);
            }
            return queryDefinition;
        }
        StringQueryDefinition queryDefinition = queryManager.newStringDefinition();
        if (this.stringQuery != null && stringQuery.length() > 0) {
            queryDefinition.setCriteria(this.stringQuery);
        }
        return queryDefinition;
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
