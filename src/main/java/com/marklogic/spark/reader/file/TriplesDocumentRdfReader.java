package com.marklogic.spark.reader.file;

import com.marklogic.spark.writer.rdf.TriplesDocument;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * So the idea is that we read the given file and create a TriplesDocument per graph. We then maintain a map
 * of graph->TriplesDocument. And we can preserve sem:origin since we know the file.
 * <p>
 * For triples, I don't think we need the graph override here. I think that only matters for writing triples, where the
 * user can say "Hey, here's my default graph if one isn't specified, or here's my override graph". And the writer
 * is best positioned to handle making the calls to create the sem:graph documents.
 * <p>
 * Except - for quads, we do need to capture the graph, and we need to know that it's not just a collection. So shoot
 * - maybe we need to add a "graph" column to a Document?
 * <p>
 * <p>
 * See https://progresssoftware.atlassian.net/wiki/spaces/PM/pages/639009040/Spark+and+RDF+data - testing verified
 * that sem:origin doesn't show up at all for quads files. And for an N3 file of 9500 triples, it does show up in each
 * of the 95 documents - but for 9800 triples or more, it doesn't show up at all. So the sem:origin element seems
 * very half-baked and not a real feature.
 */
class TriplesDocumentRdfReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InputStream inputStream;
    private TripleStreamReader tripleStreamReader;
    private InternalRow nextRowToReturn;
    private TriplesDocument currentTriplesDocument;

    TriplesDocumentRdfReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.currentTriplesDocument = null;
    }

    @Override
    public boolean next() throws IOException {
        if (tripleStreamReader == null) {
            this.inputStream = fileContext.open(filePartition);
            this.tripleStreamReader = new TripleStreamReader(filePartition.getPath(), this.inputStream);
        }
        if (!tripleStreamReader.hasNext()) {
            // This is the end state.
            return false;
        }
        while (tripleStreamReader.hasNext()) {
            // This could be a little more efficient, but fine for now.
            InternalRow tripleRow = tripleStreamReader.get();
            if (currentTriplesDocument == null) {
                currentTriplesDocument = new TriplesDocument("http://marklogic.com/semantics#default-graph", filePartition.getPath());
            }
            currentTriplesDocument.addTriple(tripleRow);
            if (currentTriplesDocument.hasMaxTriples()) {
                break;
            }
        }
        this.nextRowToReturn = new GenericInternalRow(currentTriplesDocument.toTriplesDocumentRow());
        currentTriplesDocument = null;
        return true;
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(inputStream);
    }
}
