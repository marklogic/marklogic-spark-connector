package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

class TriplesReader implements PartitionReader<InternalRow> {

    private static final String TRIPLES_QUERY = "var urisObject; " +
        "const query = cts.documentQuery(urisObject.uris); " +
        "[cts.triples(null, null, null, null, null, query)]";

    private final UriBatcher uriBatcher;
    private final DatabaseClient databaseClient;

    private Iterator<JsonNode> currentTriplesIterator;

    public TriplesReader(ForestPartition forestPartition, DocumentContext context) {
        this.databaseClient = context.isDirectConnection() ?
            context.connectToMarkLogic(forestPartition.getHost()) :
            context.connectToMarkLogic();

        SearchQueryDefinition query = context.buildTriplesSearchQuery(this.databaseClient);
        boolean filtered = false;
        if (context.hasOption(Options.READ_TRIPLES_FILTERED)) {
            filtered = Boolean.parseBoolean(context.getProperties().get(Options.READ_TRIPLES_FILTERED));
        }
        this.uriBatcher = new UriBatcher(this.databaseClient, query, forestPartition, context.getBatchSize(), filtered);
    }

    @Override
    public boolean next() throws IOException {
        final boolean haveTriplesToReturn = currentTriplesIterator != null && currentTriplesIterator.hasNext();
        if (haveTriplesToReturn) {
            return true;
        }

        while (currentTriplesIterator == null || !currentTriplesIterator.hasNext()) {
            List<String> uris = uriBatcher.nextBatchOfUris();
            if (uris.isEmpty()) {
                return false; // End state; no more matching documents were found.
            }
            readNextArrayOfTriples(uris);
        }
        return true;
    }

    @Override
    public InternalRow get() {
        Object[] row = convertNextTripleIntoRow();
        return new GenericInternalRow(row);
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private void readNextArrayOfTriples(List<String> uris) {
        ObjectNode urisObject = new ObjectMapper().createObjectNode();
        ArrayNode urisArray = urisObject.putArray("uris");
        uris.forEach(urisArray::add);

        // The eval result is an array wrapping an array of triples.
        JsonNode result = this.databaseClient.newServerEval().javascript(TRIPLES_QUERY)
            .addVariableAs("urisObject", new JacksonHandle(urisObject))
            .evalAs(JsonNode.class);
        JsonNode triplesArray = result.get(0);
        currentTriplesIterator = triplesArray.iterator();
    }

    private Object[] convertNextTripleIntoRow() {
        JsonNode triple = currentTriplesIterator.next().get("triple");
        Object[] row = new Object[6];
        row[0] = UTF8String.fromString(triple.get("subject").asText());
        row[1] = UTF8String.fromString(triple.get("predicate").asText());
        JsonNode object = triple.get("object");
        if (object.has("value")) {
            row[2] = UTF8String.fromString(object.get("value").asText());
            if (object.has("datatype")) {
                row[3] = UTF8String.fromString(object.get("datatype").asText());
            }
            if (object.has("lang")) {
                row[4] = UTF8String.fromString(object.get("lang").asText());
            }
        } else {
            row[2] = UTF8String.fromString(object.asText());
        }
        return row;
    }
}
