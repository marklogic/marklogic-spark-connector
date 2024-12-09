/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that DSL queries with accessors other than "fromView" will succeed, as of the 2.5.0 release.
 */
class ReadWithAccessorOtherThanFromViewTest extends AbstractIntegrationTest {

    @Test
    void fromSearchDocs() {
        List<Row> rows = readWithQuery("op.fromSearchDocs(cts.collectionQuery('author'))")
            .load()
            .collectAsList();

        assertEquals(15, rows.size());

        StructType schema = readWithQuery("op.fromSearchDocs(cts.collectionQuery('author'))")
            .load()
            .schema();
        StructField uriField = schema.fields()[schema.fieldIndex("uri")];
        assertEquals(DataTypes.StringType, uriField.dataType(), "Just verifying that the connector will infer a " +
            "schema for a non-fromView query without issue, as the schema inference is based on columnInfo which is " +
            "not tied to the data access function.");
    }

    @Test
    void fromSearchDocsWithCount() {
        long count = readWithQuery("op.fromSearchDocs(cts.collectionQuery('author'))")
            .load()
            .limit(10)
            .count();

        assertEquals(10, count, "This test verifies both that a 'limit' filter can be pushed down and also that " +
            "the 'count' call is pushed down correctly with a 'groupBy' being added as the last operator to the " +
            "Optic plan. For non-fromView queries, new operators should always be appended to the end of the " +
            "Optic plan since there's no op:prepare call that was added by the internal/viewinfo endpoint.");
    }

    @Test
    void fromLexicons() {
        List<Row> rows = readWithQuery("op.fromLexicons({" +
            "  'CitationID': cts.elementReference('CitationID')" +
            "})" +
            ".groupBy('CitationID', op.count('counts'))")
            .load()
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());

        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(4, rows.get(0).getLong(1), "Should be 4 rows with CitationID=1.");
    }

    private DataFrameReader readWithQuery(String query) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.CLIENT_URI, makeClientUri());
    }
}
