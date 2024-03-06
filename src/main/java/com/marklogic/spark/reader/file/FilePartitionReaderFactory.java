package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
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

        final String fileType = fileContext.getStringOption(Options.READ_FILES_TYPE);
        if ("rdf".equalsIgnoreCase(fileType)) {
            return createRdfReader(filePartition);
        } else if ("mlcp_archive".equalsIgnoreCase(fileType)) {
            return new MlcpArchiveFileReader(filePartition, fileContext);
        } else if (fileContext.hasOption(Options.READ_AGGREGATES_XML_ELEMENT)) {
            return fileContext.isZip() ?
                new ZipAggregateXMLFileReader(filePartition, fileContext) :
                new AggregateXMLFileReader(filePartition, fileContext);
        } else if (fileContext.isZip()) {
            return new ZipFileReader(filePartition, fileContext);
        } else if (fileContext.isGzip()) {
            return new GZIPFileReader(filePartition, fileContext);
        }

        throw new ConnectorException(String.format("File is not supported: %s", filePartition.getPath()));
    }

    private PartitionReader<InternalRow> createRdfReader(FilePartition filePartition) {
        if (fileContext.isZip()) {
            return new RdfZipFileReader(filePartition, fileContext);
        }
        return RdfUtil.isQuadsFile(filePartition.getPath()) ?
            new QuadsFileReader(filePartition, fileContext) :
            new TriplesFileReader(filePartition, fileContext);
    }
}
