/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Knows how to convert a stream of Jena Triple objects into Spark rows.
 */
class TripleStreamReader implements RdfStreamReader {

    private final String path;
    private final Iterator<Triple> tripleStream;
    private final RdfSerializer rdfSerializer = new RdfSerializer();

    TripleStreamReader(String path, InputStream inputStream) {
        this.path = path;
        RDFParserBuilder parserBuilder = RDFParserBuilder.create()
            .source(inputStream)
            .errorHandler(new RdfErrorHandler(path))
            .lang(determineLang(path))
            .base(path);
        this.tripleStream = AsyncParser.of(parserBuilder).streamTriples().iterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            return this.tripleStream.hasNext();
        } catch (RiotException e) {
            if (e.getMessage().contains("Failed to determine the RDF syntax")) {
                throw new ConnectorException(String.format("Unable to read file at %s; RDF syntax is not supported or " +
                    "the file extension is not recognized.", this.path), e);
            }
            throw new ConnectorException(String.format("Unable to read %s; cause: %s", this.path, e.getMessage()), e);
        }
    }

    @Override
    public InternalRow get() {
        // Per the Jena javadocs, next() is not expected to throw an error; if an were to occur, it would occur when
        // calling hasNext() first.
        return rdfSerializer.serialize(this.tripleStream.next());
    }

    /**
     * This is only defining extensions that Jena does not appear to recognize. Testing has shown that providing the
     * file path for the Jena {@code base} method will work for all the file types we support - except for RDF JSON,
     * we need to Jena that ".json" maps to RDF JSON.
     *
     * @param path
     * @return
     */
    private Lang determineLang(String path) {
        if (path.endsWith(".json") || path.endsWith(".json.gz")) {
            return Lang.RDFJSON;
        } else if (path.endsWith(".thrift")) {
            return Lang.RDFTHRIFT;
        } else if (path.endsWith(".binpb")) {
            // See https://protobuf.dev/programming-guides/techniques/#suffixes .
            return Lang.RDFPROTO;
        }
        return null;
    }
}
