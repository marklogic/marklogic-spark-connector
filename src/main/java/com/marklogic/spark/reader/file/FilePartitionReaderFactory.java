package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class FilePartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private final FileContext fileContext;

    FilePartitionReaderFactory(FileContext fileContext) {
        this.fileContext = fileContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        FilePartition filePartition = (FilePartition) partition;

        if ("rdf".equalsIgnoreCase(fileContext.getStringOption(Options.READ_FILES_TYPE))) {
            return createRdfReader(filePartition);
        }

        if ("mlcp_archive".equalsIgnoreCase(fileContext.getStringOption(Options.READ_FILES_TYPE))) {
            return new MlcpArchiveFileReader(filePartition, fileContext);
        }

        final boolean isZip = fileContext.isZip();
        final boolean isGzip = fileContext.isGzip();

        String aggregateXmlElement = fileContext.getStringOption(Options.READ_AGGREGATES_XML_ELEMENT);
        if (aggregateXmlElement != null && !aggregateXmlElement.trim().isEmpty()) {
            if (isZip) {
                return new ZipAggregateXMLFileReader(filePartition, fileContext);
            } else if (isGzip) {
                return new GZIPAggregateXMLFileReader(filePartition, fileContext);
            }
            return new AggregateXMLFileReader(filePartition, fileContext);
        } else if (isZip) {
            return new ZipFileReader(filePartition, fileContext);
        } else if (isGzip) {
            return new GZIPFileReader(filePartition, fileContext);
        }
        throw new ConnectorException("Only zip and gzip files supported, more to come before 2.2.0 release.");
    }

    private PartitionReader<InternalRow> createRdfReader(FilePartition filePartition) {
        if (fileContext.isZip()) {
            return new RdfZipFileReader(filePartition, fileContext);
        }
        final Path path = new Path(filePartition.getPath());
        return RdfUtil.isQuadsFile(path.getName()) ?
            new QuadsFileReader(filePartition, fileContext) :
            new TriplesFileReader(filePartition, fileContext);
    }
}
