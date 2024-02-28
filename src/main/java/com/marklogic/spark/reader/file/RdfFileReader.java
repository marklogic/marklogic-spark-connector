package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class RdfFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final Iterator<Triple> tripleStream;
    private final FilePartition partition;

    RdfFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration, Map<String, String> properties) {
        super(partition);
        this.partition = partition;
        final Path path = new Path(partition.getPath());
        
        try {
            this.inputStream = openStream(path, hadoopConfiguration, properties);
            RDFParserBuilder parserBuilder = RDFParserBuilder.create()
                .source(this.inputStream)
                .errorHandler(new RdfErrorHandler(partition.getPath()))
                .lang(determineLang(partition))
                .base(path.getName());
            this.tripleStream = AsyncParser.of(parserBuilder).streamTriples().iterator();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        try {
            return this.tripleStream.hasNext();
        } catch (RiotException e) {
            if (e.getMessage().contains("Failed to determine the RDF syntax")) {
                throw new ConnectorException(String.format("Unable to read RDF file at %s; RDF syntax is not supported or " +
                    "the file extension is not recognized.", partition.getPath()), e);
            }
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s",
                partition.getPath(), e.getMessage()), e);
        }
    }

    @Override
    public InternalRow get() {
        return rdfSerializer.serialize(this.tripleStream.next());
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }

    /**
     * This is only defining extensions that Jena does not appear to recognize. Testing has shown that providing the
     * file path for the Jena {@code base} method will work for all the file types we support - except for RDF JSON,
     * we need to Jena that ".json" maps to RDF JSON.
     *
     * @param partition
     * @return
     */
    private Lang determineLang(FilePartition partition) {
        String path = partition.getPath().toLowerCase();
        if (path.endsWith(".json") || path.endsWith(".json.gz")) {
            return Lang.RDFJSON;
        }
        return null;
    }
}
