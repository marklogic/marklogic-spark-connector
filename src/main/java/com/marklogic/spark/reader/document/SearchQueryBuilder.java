package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.SearchQueryDefinition;

import java.util.Arrays;

/**
 * Potentially reusable class for the Java Client that handles constructing a query based on a common
 * set of user-defined inputs.
 */
public class SearchQueryBuilder {

    public enum QueryType {
        STRING,
        STRUCTURED,
        SERIALIZED_CTS,
        COMBINED;
    }

    private String query;
    private QueryType queryType;
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

    public SearchQueryBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public SearchQueryBuilder withQueryType(String queryType) {
        if (queryType != null) {
            try {
                this.queryType = QueryType.valueOf(queryType.toUpperCase());
            } catch (IllegalArgumentException e) {
                String message = String.format("Invalid query type: %s; must be one of %s", queryType, Arrays.asList(QueryType.values()));
                throw new IllegalArgumentException(message);
            }
        }
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
        if (query != null) {
            if (QueryType.STRUCTURED.equals(queryType)) {
                return queryManager.newRawStructuredQueryDefinition(new StringHandle(query));
            } else if (QueryType.SERIALIZED_CTS.equals(queryType)) {
                return queryManager.newRawCtsQueryDefinition(new StringHandle(query));
            } else if (QueryType.COMBINED.equals(queryType)) {
                return queryManager.newRawCombinedQueryDefinition(new StringHandle(query));
            } else {
                return queryManager.newStringDefinition().withCriteria(this.query);
            }
        }
        return queryManager.newStringDefinition();
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
