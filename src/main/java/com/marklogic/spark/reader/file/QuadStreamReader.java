package com.marklogic.spark.reader.file;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
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

    private final Iterator<Quad> quadStream;
    private final RdfSerializer rdfSerializer = new RdfSerializer();

    QuadStreamReader(String path, InputStream inputStream) {
        final Lang lang = path.endsWith(".trig") || path.endsWith(".trig.gz") ? Lang.TRIG : Lang.NQ;
        this.quadStream = AsyncParser.of(RDFParserBuilder.create()
            .source(inputStream)
            .lang(lang)
            .errorHandler(new RdfErrorHandler(path))
            .base(path)
        ).streamQuads().iterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        return this.quadStream.hasNext();
    }

    @Override
    public InternalRow get() {
        return rdfSerializer.serialize(this.quadStream.next());
    }
}
