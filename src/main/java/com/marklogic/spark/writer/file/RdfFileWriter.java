package com.marklogic.spark.writer.file;

import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

class RdfFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(RdfFileWriter.class);

    private final Map<String, String> properties;
    private final String path;
    private final SerializableConfiguration hadoopConfiguration;
    private final int partitionId;
    private final String graph;

    private OutputStream outputStream;
    private StreamRDF stream;

    RdfFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.properties = properties;
        this.path = properties.get("path");
        this.graph = properties.get(Options.WRITE_RDF_FILES_GRAPH);
        this.hadoopConfiguration = hadoopConfiguration;
        this.partitionId = partitionId;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (outputStream == null) {
            createStream();
        }

        Resource subject = ResourceFactory.createResource(row.getString(0));
        Property predicate = ResourceFactory.createProperty(row.getString(1));
        RDFNode object;
        if (row.isNullAt(3)) {
            object = ResourceFactory.createResource(row.getString(2));
        } else {
            String datatype = row.getString(3);
            object = ResourceFactory.createTypedLiteral(row.getString(2), new BaseDatatype(datatype));
        }

        Triple triple = ResourceFactory.createStatement(subject, predicate, object).asTriple();
        if (this.graph == null) {
            this.stream.triple(triple);
        } else {
            this.stream.quad(new Quad(ResourceFactory.createResource(this.graph).asNode(), triple));
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        this.stream.finish();
        return null;
    }

    @Override
    public void abort() {
        // Nothing to do yet.
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.outputStream);
    }

    private void createStream() throws IOException {
        LangAndExtension langAndExtension = determineLangAndExtension();
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        String filename = String.format("%s-%d.%s", timestamp, partitionId, langAndExtension.extension);
        Path filePath = new Path(this.path, filename);
        logger.info("Will write to: {}", filePath.toUri());

        FileSystem fileSystem = filePath.getFileSystem(this.hadoopConfiguration.value());
        fileSystem.setWriteChecksum(false);
        if (fileSystem instanceof LocalFileSystem) {
            File file = new File(filePath.toUri().getPath());
            if (!file.exists() && file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            this.outputStream = new BufferedOutputStream(new FileOutputStream(file, false));
        } else {
            this.outputStream = new BufferedOutputStream(fileSystem.create(filePath, false));
        }
        this.stream = StreamRDFWriter.getWriterStream(this.outputStream, langAndExtension.lang);
        this.stream.start();
    }

    private LangAndExtension determineLangAndExtension() {
        RdfContext context = new RdfContext(properties);
        if (context.hasOption(Options.WRITE_RDF_FILES_FORMAT)) {
            String value = context.getStringOption(Options.WRITE_RDF_FILES_FORMAT);
            if ("trig".equalsIgnoreCase(value)) {
                return new LangAndExtension(Lang.TRIG, "trig");
            } else if ("nt".equalsIgnoreCase(value) || "ntriples".equalsIgnoreCase(value)) {
                return new LangAndExtension(Lang.NTRIPLES, "nt");
            } else if ("nq".equalsIgnoreCase(value) || "nquads".equalsIgnoreCase(value)) {
                return new LangAndExtension(Lang.NQUADS, "nq");
            } else if ("trix".equalsIgnoreCase(value)) {
                return new LangAndExtension(Lang.TRIX, "trix");
            } else if ("rdfthrift".equalsIgnoreCase(value)) {
                return new LangAndExtension(Lang.RDFTHRIFT, "thrift");
            }
        }
        return new LangAndExtension(Lang.TTL, "ttl");
    }

    private static class RdfContext extends ContextSupport {
        private RdfContext(Map<String, String> properties) {
            super(properties);
        }
    }

    private static class LangAndExtension {
        private final Lang lang;
        private final String extension;

        LangAndExtension(Lang lang, String extension) {
            this.lang = lang;
            this.extension = extension;
        }
    }
}
