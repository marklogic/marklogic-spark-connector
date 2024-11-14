/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.Options;
import com.marklogic.spark.reader.file.xml.AggregateXmlFileReader;
import com.marklogic.spark.reader.file.xml.ZipAggregateXmlFileReader;
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
        final FilePartition filePartition = (FilePartition) partition;
        final String fileType = fileContext.getStringOption(Options.READ_FILES_TYPE);

        if ("rdf".equalsIgnoreCase(fileType)) {
            if (fileContext.isZip()) {
                return new RdfZipFileReader(filePartition, fileContext);
            }
            return new RdfFileReader(filePartition, fileContext);
        } else if ("mlcp_archive".equalsIgnoreCase(fileType)) {
            return new MlcpArchiveFileReader(filePartition, fileContext);
        } else if ("archive".equalsIgnoreCase(fileType)) {
            return new ArchiveFileReader(filePartition, fileContext);
        } else if ("json_lines".equalsIgnoreCase(fileType)) {
            return new JsonLinesFileReader(filePartition, fileContext);
        } else if (fileContext.hasOption(Options.READ_AGGREGATES_XML_ELEMENT)) {
            return fileContext.isZip() ?
                new ZipAggregateXmlFileReader(filePartition, fileContext) :
                new AggregateXmlFileReader(filePartition, fileContext);
        } else if (fileContext.isZip()) {
            return new ZipFileReader(filePartition, fileContext);
        } else if (fileContext.isGzip()) {
            return new GzipFileReader(filePartition, fileContext);
        }
        return new GenericFileReader(filePartition, fileContext);
    }
}
