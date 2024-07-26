package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
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
import java.util.zip.GZIPOutputStream;

class RdfFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(RdfFileWriter.class);

    private final RdfContext rdfContext;
    private final String path;
    private final SerializableConfiguration hadoopConfiguration;
    private final int partitionId;
    private final LangAndExtension langAndExtension;
    private final String graphOverride;
    private final boolean isGZIP;

    private OutputStream outputStream;
    private StreamRDF stream;

    RdfFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.rdfContext = new RdfContext(properties);
        this.path = properties.get("path");
        this.graphOverride = rdfContext.getStringOption(Options.WRITE_RDF_FILES_GRAPH);
        this.hadoopConfiguration = hadoopConfiguration;
        this.partitionId = partitionId;
        this.langAndExtension = determineLangAndExtension();

        String value = rdfContext.getStringOption(Options.WRITE_FILES_COMPRESSION);
        if ("gzip".equals(value)) {
            this.isGZIP = true;
        } else if (value != null && value.trim().length() > 0) {
            throw new ConnectorException(String.format("Unsupported compression value; only 'gzip' is supported: %s", value));
        } else {
            this.isGZIP = false;
        }

        if (!this.langAndExtension.supportsGraph() && this.graphOverride != null) {
            Util.MAIN_LOGGER.warn("RDF graph '{}' will be ignored since the target RDF format of '{}' does not support graphs.",
                this.graphOverride, this.langAndExtension.lang.getName());
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (outputStream == null) {
            createStream();
        }

        final Triple triple = makeTriple(row);
        final String graph = determineGraph(row);
        if (graph == null || !this.langAndExtension.supportsGraph()) {
            this.stream.triple(triple);
        } else {
            this.stream.quad(new Quad(ResourceFactory.createResource(graph).asNode(), triple));
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (this.stream != null) {
            this.stream.finish();
        }
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
        final Path filePath = makeFilePath();
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

        if (isGZIP) {
            this.outputStream = new GZIPOutputStream(this.outputStream);
        }

        this.stream = StreamRDFWriter.getWriterStream(this.outputStream, langAndExtension.lang);
        this.stream.start();
    }

    private Path makeFilePath() {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        String filename = String.format("%s-%d.%s", timestamp, partitionId, langAndExtension.extension);
        if (this.isGZIP) {
            filename += ".gz";
        }
        return new Path(this.path, filename);
    }

    /**
     * See https://jena.apache.org/documentation/io/streaming-io.html#rdfformat-and-lang . Tried using RDFFormat for
     * more choices, but oddly ran into errors when using TTL and a couple other formats. Lang seems to work just fine.
     *
     * @return
     */
    private LangAndExtension determineLangAndExtension() {
        if (rdfContext.hasOption(Options.WRITE_RDF_FILES_FORMAT)) {
            String value = rdfContext.getStringOption(Options.WRITE_RDF_FILES_FORMAT);
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
            } else if ("rdfproto".equalsIgnoreCase(value)) {
                // See https://protobuf.dev/programming-guides/techniques/#suffixes .
                return new LangAndExtension(Lang.RDFPROTO, "binpb");
            }
        }
        return new LangAndExtension(Lang.TTL, "ttl");
    }

    private Triple makeTriple(InternalRow row) {
        Resource subject = ResourceFactory.createResource(row.getString(0));
        Property predicate = ResourceFactory.createProperty(row.getString(1));
        RDFNode object = makeObjectNode(row);
        return ResourceFactory.createStatement(subject, predicate, object).asTriple();
    }

    private RDFNode makeObjectNode(InternalRow row) {
        final String objectValue = row.getString(2);
        final int datatypeIndex = 3;
        if (row.isNullAt(datatypeIndex)) {
            return ResourceFactory.createResource(objectValue);
        }
        String datatype = row.getString(datatypeIndex);
        String lang = row.isNullAt(4) ? null : row.getString(4);
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".equals(datatype) ?
            ResourceFactory.createLangLiteral(objectValue, lang) :
            ResourceFactory.createTypedLiteral(objectValue, new BaseDatatype(datatype));
    }

    private String determineGraph(InternalRow row) {
        if (this.graphOverride != null) {
            return this.graphOverride;
        }
        return row.isNullAt(5) ? null : row.getString(5);
    }

    // Exists so we can use the convenience methods on ContextSupport.
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

        boolean supportsGraph() {
            return lang.equals(Lang.TRIG) || lang.equals(Lang.NQUADS) || lang.equals(Lang.TRIX);
        }
    }
}
