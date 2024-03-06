package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Knows how to convert a stream of Jena Quad objects into Spark rows.
 */
class QuadStreamReader implements RdfStreamReader {

    private final String path;
    private final Iterator<Quad> quadStream;
    private final RdfSerializer rdfSerializer = new RdfSerializer();

    QuadStreamReader(String path, InputStream inputStream) {
        this.path = path;
        final Lang lang = RdfUtil.isTrigFile(path) ? Lang.TRIG : Lang.NQ;
        this.quadStream = AsyncParser.of(RDFParserBuilder.create()
            .source(inputStream)
            .lang(lang)
            .errorHandler(new RdfErrorHandler(path))
            .base(path)
        ).streamQuads().iterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            return this.quadStream.hasNext();
        } catch (RiotException e) {
            throw new ConnectorException(String.format("Unable to read file at %s; cause: %s",
                this.path, e.getMessage()), e);
        }
    }

    @Override
    public InternalRow get() {
        return rdfSerializer.serialize(this.quadStream.next());
    }
}
