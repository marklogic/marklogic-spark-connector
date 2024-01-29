package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

class DocumentContext extends ContextSupport {

    DocumentContext(CaseInsensitiveStringMap options) {
        super(options.asCaseSensitiveMap());
    }

    Set<DocumentManager.Metadata> getRequestedMetadata() {
        Set<DocumentManager.Metadata> set = new HashSet<>();
        if (hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            for (String category : getProperties().get(Options.READ_DOCUMENTS_CATEGORIES).split(",")) {
                if ("content".equalsIgnoreCase(category)) {
                    continue;
                }
                if ("metadata".equalsIgnoreCase(category)) {
                    set.add(DocumentManager.Metadata.ALL);
                } else {
                    set.add(DocumentManager.Metadata.valueOf(category.toUpperCase()));
                }
            }
        }
        return set;
    }

    boolean contentWasRequested() {
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
        return new SearchQueryBuilder()
            .withQuery(getProperties().get(Options.READ_DOCUMENTS_QUERY))
            .withQueryType(getProperties().get(Options.READ_DOCUMENTS_QUERY_TYPE))
            .withCollections(getProperties().get(Options.READ_DOCUMENTS_COLLECTIONS))
            .buildQuery(client);
    }
}
