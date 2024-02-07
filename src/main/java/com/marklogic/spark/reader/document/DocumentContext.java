package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
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
        final Map<String, String> props = getProperties();
        return new SearchQueryBuilder()
            .withStringQuery(props.get(Options.READ_DOCUMENTS_STRING_QUERY))
            .withQuery(props.get(Options.READ_DOCUMENTS_QUERY))
            .withQueryFormat(props.get(Options.READ_DOCUMENTS_QUERY_FORMAT))
            .withCollections(props.get(Options.READ_DOCUMENTS_COLLECTIONS))
            .withDirectory(props.get(Options.READ_DOCUMENTS_DIRECTORY))
            .withOptionsName(props.get(Options.READ_DOCUMENTS_OPTIONS))
            .withTransformName(props.get(Options.READ_DOCUMENTS_TRANSFORM))
            .withTransformParams(props.get(Options.READ_DOCUMENTS_TRANSFORM_PARAMS))
            .withTransformParamsDelimiter(props.get(Options.READ_DOCUMENTS_TRANSFORM_PARAMS_DELIMITER))
            .buildQuery(client);
    }

    int getBatchSize() {
        // Testing has shown that at least for smaller documents, 100 or 200 can be significantly slower than something
        // like 1000 or even 10000. 500 is thus used as a default that should still be reasonably performant for larger
        // documents.
        int defaultBatchSize = 500;
        return (int) getNumericOption(Options.READ_BATCH_SIZE, defaultBatchSize, 1);
    }

    int getPartitionsPerForest() {
        int defaultPartitionsPerForest = 2;
        return (int) getNumericOption(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, defaultPartitionsPerForest, 1);
    }
}
