package com.marklogic.spark.writer.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;

import java.io.*;
import java.util.Map;

class RdfFileWriter implements DataWriter<InternalRow> {

    private final String path;
    private final SerializableConfiguration hadoopConfiguration;
    private OutputStream outputStream;
    private StreamRDF stream;

    public RdfFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.path = properties.get("path");
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (outputStream == null) {
            Path filePath = new Path(this.path, "test.ttl");
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
            this.stream = StreamRDFWriter.getWriterStream(this.outputStream, Lang.TURTLE);
            this.stream.start();
        }

        Resource subject = ResourceFactory.createResource(row.getString(0));
        Property predicate = ResourceFactory.createProperty(row.getString(1));
        RDFNode object;
        if (row.isNullAt(3)) {
            object = ResourceFactory.createResource(row.getString(2));
        } else {
            String datatype = row.getString(3);
            object = ResourceFactory.createStringLiteral(row.getString(2));
        }
        Triple triple = ResourceFactory.createStatement(subject, predicate, object).asTriple();
        this.stream.triple(triple);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        this.stream.finish();
        return null;
    }

    @Override
    public void abort() {

    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.outputStream);
    }
}
