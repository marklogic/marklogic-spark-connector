package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

class QuadsFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(QuadsFileReader.class);

    private final InputStream inputStream;
    private final Iterator<Quad> quadStream;

    private final RdfSerializer rdfSerializer = new RdfSerializer();

    QuadsFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration) {
        if (logger.isDebugEnabled()) {
            logger.debug("Reading RDF file {}", partition.getPath());
        }
        Path path = new Path(partition.getPath());
        Lang lang = path.getName().endsWith(".trig") ? Lang.TRIG : Lang.NQ;
        try {
            this.inputStream = path.getFileSystem(hadoopConfiguration.value()).open(path);
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

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }
}
