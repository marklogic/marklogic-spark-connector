package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

class RdfFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(RdfFileReader.class);

    private final InputStream inputStream;
    private final Iterator<Triple> tripleStream;

    private final TripleSerializer tripleSerializer = new TripleSerializer();

    RdfFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration) {
        if (logger.isDebugEnabled()) {
            logger.debug("Reading RDF file {}", partition.getPath());
        }
        Path path = new Path(partition.getPath());
        try {
            this.inputStream = path.getFileSystem(hadoopConfiguration.value()).open(path);
            this.tripleStream = AsyncParser.of(RDFParserBuilder.create()
                .source(this.inputStream)
                .lang(Lang.RDFXML)
                .base(partition.getPath())
            ).streamTriples().iterator();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return this.tripleStream.hasNext();
    }

    @Override
    public InternalRow get() {
        Triple triple = this.tripleStream.next();
        return tripleSerializer.serialize(triple);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }
}
