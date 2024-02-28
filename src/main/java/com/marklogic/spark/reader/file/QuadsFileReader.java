package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class QuadsFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final Iterator<Quad> quadStream;

    QuadsFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration, Map<String, String> properties) {
        super(partition);

        final Path path = new Path(partition.getPath());
        final Lang lang = path.getName().endsWith(".trig") || path.getName().endsWith(".trig.gz") ? Lang.TRIG : Lang.NQ;

        try {
            this.inputStream = openStream(path, hadoopConfiguration, properties);
            // MLCP uses RiotParsers.createIteratorNQuads for NQ, but it is not known why. This approach appears
            // to work fine for both TRIG and NQ.
            // See https://jena.apache.org/documentation/io/rdf-input.html for more information.
            this.quadStream = AsyncParser.of(RDFParserBuilder.create()
                .source(this.inputStream)
                .lang(lang)
                .errorHandler(new RdfErrorHandler(partition.getPath()))
                .base(partition.getPath())
            ).streamQuads().iterator();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return this.quadStream.hasNext();
    }

    @Override
    public InternalRow get() {
        return rdfSerializer.serialize(this.quadStream.next());
    }
}
