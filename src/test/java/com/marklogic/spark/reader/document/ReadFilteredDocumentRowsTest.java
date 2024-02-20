package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * As touched in the documentation for this feature, filtering can in some scenarios significantly improve performance
 * by not retrieving a large number of false positives. Generally, as the percentage of false positives increases,
 * the benefit from filtering will increase by causing the connector to retrieve fewer documents. Overall though,
 * we would still recommend to a customer to configure their indexes so that they can use an unfiltered query that is
 * both fast and accurate.
 */
class ReadFilteredDocumentRowsTest extends AbstractIntegrationTest {

    private static final String FALSE_POSITIVE_QUERY = "<json-property-word-query xmlns='http://marklogic.com/cts'>" +
        "<property>ForeName</property>" +
        "<text xml:lang='en'>Wool*</text>" +
        "</json-property-word-query>";

    private static final String CORRECT_WILDCARD_QUERY = "<json-property-word-query xmlns='http://marklogic.com/cts'>" +
        "<property>LastName</property>" +
        "<text xml:lang='en'>Wool*</text>" +
        "</json-property-word-query>";

    @Test
    void falsePositive() {
        DataFrameReader reader = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_QUERY, FALSE_POSITIVE_QUERY);

        Dataset<Row> dataset = reader.load();

        assertEquals(1, dataset.count(), "The database has trailing-wildcard-searches enabled, which allows for " +
            "'Wool*' to work. But since the search is unfiltered, we get a false positive as 'Wooles' appears in " +
            "the LastName property, not the ForeName property.");

        dataset = reader.option(Options.READ_DOCUMENTS_FILTERED, "true").load();

        assertEquals(0, dataset.count(), "Now that the search is filtered, the false positive will be omitted.");
    }

    @Test
    void correctWildcardQuery() {
        DataFrameReader reader = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_QUERY, CORRECT_WILDCARD_QUERY);

        Dataset<Row> dataset = reader.load();
        assertEquals(1, dataset.count());

        dataset = reader.option(Options.READ_DOCUMENTS_FILTERED, "true").load();
        assertEquals(1, dataset.count(), "This test just verifies that a valid wildcard query works correctly on " +
            "our test database.");
    }

    @Test
    void invalidValue() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_QUERY, FALSE_POSITIVE_QUERY)
            .option(Options.READ_DOCUMENTS_FILTERED, "not-valid")
            .load();

        assertEquals(1, dataset.count(), "Boolean.parseBoolean interprets a non-true/false value as false, so we " +
            "expect the query to be unfiltered and thus we get back a count of 1 due to the false positive.");
    }
}
