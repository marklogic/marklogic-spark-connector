package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class RdfZipFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(RdfZipFileReader.class);

    private final CustomZipInputStream customZipInputStream;
    private final String path;

    private RdfStreamReader rdfStreamReader;

    RdfZipFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration) {
        this.path = partition.getPath();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading path: {}", this.path);
        }
        try {
            Path hadoopPath = new Path(partition.getPath());
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            this.customZipInputStream = new CustomZipInputStream(fileSystem.open(hadoopPath));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read %s; cause: %s", this.path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        if (rdfStreamReader != null && rdfStreamReader.hasNext()) {
            return true;
        }

        // Loop until we find a non-empty RDF file, or we run out of zip entries.
        while (true) {
            ZipEntry zipEntry = FileUtil.findNextFileEntry(customZipInputStream);
            if (zipEntry == null) {
                return false;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Reading entry {} in {}", zipEntry.getName(), this.path);
            }

            this.rdfStreamReader = RdfUtil.isQuadsFile(zipEntry.getName()) ?
                new QuadStreamReader(zipEntry.getName(), customZipInputStream) :
                new TripleStreamReader(zipEntry.getName(), customZipInputStream);

            if (this.rdfStreamReader.hasNext()) {
                return true;
            }
        }
    }

    @Override
    public InternalRow get() {
        return this.rdfStreamReader.get();
    }

    @Override
    public void close() throws IOException {
        if (this.customZipInputStream != null) {
            this.customZipInputStream.readyToClose = true;
            IOUtils.closeQuietly(this.customZipInputStream);
        }
    }

    /**
     * Per https://jena.apache.org/documentation/io/rdf-input.html#iterating-over-parser-output , Jena will call
     * close() on an iterator after reading all the triples/quads. This results in the ZipInputStream being closed,
     * which prevents any additional entries from being read.
     * <p>
     * We know we only want to close the stream when Spark calls close() or abort(). So this modifies the close() method
     * to only close after a boolean has been flipped to true.
     */
    private static class CustomZipInputStream extends ZipInputStream {

        private boolean readyToClose = false;

        public CustomZipInputStream(@NotNull InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            if (readyToClose) {
                super.close();
            }
        }
    }
}
