package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadDocumentRowsWithPartitionCountsTest extends AbstractIntegrationTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 16})
    void test(int partitionCount) {
        Dataset<Row> dataset = readAuthors(partitionCount);
        assertEquals(15, dataset.count(), "Regardless of the partition count, the 15 authors " +
            "should be returned.");
    }

    @Test
    void wayTooManyPartitions() {
        Dataset<Row> dataset = readAuthors(500);
        assertEquals(15, dataset.count(), "Under the hood, the connector is expected to create 45 partitions, as " +
            "there are expected to be 3 forests and there are 15 matching documents. So the most possible partitions " +
            "for a forest would be 15, and thus 45 is the max. Regardless, this is expected to work and still return " +
            "only the 15 matching author documents - it'll just be a little slower with so many partitions.");
    }

    @Test
    void zeroPartitions() {
        Dataset<Row> dataset = readAuthors(0);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> dataset.count());
    }

    @Test
    void invalidValue() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, "abc")
            .load();

        ConnectorException ex = assertThrows(ConnectorException.class, () -> dataset.count());
        assertEquals("Value of 'spark.marklogic.read.documents.partitionsPerForest' option must be numeric.", ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{\"ctsquery\": {\"collectionQuery\": {\"uris\": [\"author\"]}}}",
        "{\"query\": {\"collection-query\": {\"uri\": [\"author\"]}}}",
        "{\"search\": {\"query\": {\"collection-query\": {\"uri\": [\"author\"]}}}}"
    })
    void complexQuery(String query) {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 3)
            .load()
            .count();

        assertEquals(15, count, "Unexpected count for query: " + query);
    }

    private Dataset<Row> readAuthors(int partitionsPerForest) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, partitionsPerForest)
            .load();
    }
}
