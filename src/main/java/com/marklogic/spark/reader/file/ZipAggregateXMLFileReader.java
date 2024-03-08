package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ZipAggregateXMLFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipAggregateXMLFileReader.class);

    private final FileContext fileContext;
    private final ZipInputStream zipInputStream;
    private final String path;

    private AggregateXMLSplitter aggregateXMLSplitter;

    // Used solely for a default URI prefix.
    private int entryCounter;

    private InternalRow rowToReturn;

    ZipAggregateXMLFileReader(FilePartition filePartition, FileContext fileContext) {
        this.fileContext = fileContext;
        this.path = filePartition.getPath();
        this.zipInputStream = new ZipInputStream(fileContext.open(filePartition));
    }

    /**
     * Finds the next valid XML element from either the current zip entry or the next valid zip entry.
     *
     * @return
     * @throws IOException
     */
    @Override
    public boolean next() throws IOException {
        while (true) {
            // If we don't already have a splitter open on a zip entry, find the next valid zip entry to process.
            if (aggregateXMLSplitter == null) {
                boolean foundZipEntry = findNextValidZipEntry();
                if (!foundZipEntry) {
                    return false;
                }
            }

            // If we have a splitter open on a zip entry, find the next valid row to return from the entry.
            if (aggregateXMLSplitter != null) {
                boolean foundRow = findNextRowToReturn();
                if (foundRow) {
                    return true;
                }
            }
        }
    }

    @Override
    public InternalRow get() {
        return this.rowToReturn;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.zipInputStream);
    }

    /**
     * Find the next valid entry in the zip file. The most likely reason an entry will fail is because it's not XML
     * or not a valid XML document.
     *
     * @return false if there are no more valid entries in the zip; true otherwise.
     * @throws IOException
     */
    private boolean findNextValidZipEntry() {
        while (true) {
            // Once we no longer have any valid zip entries, we're done.
            ZipEntry zipEntry;
            try {
                zipEntry = FileUtil.findNextFileEntry(zipInputStream);
            } catch (IOException e) {
                String message = String.format("Unable to read zip entry from %s; cause: %s", this.path, e.getMessage());
                if (fileContext.isReadAbortOnFailure()) {
                    throw new ConnectorException(message, e);
                }
                Util.MAIN_LOGGER.warn(message);
                return false;
            }

            if (zipEntry == null) {
                return false;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Reading entry {} in {}", zipEntry.getName(), this.path);
            }
            entryCounter++;
            String identifierForError = "entry " + zipEntry.getName() + " in " + this.path;

            try {
                aggregateXMLSplitter = new AggregateXMLSplitter(identifierForError, this.zipInputStream, this.fileContext.getProperties());
                // Fail fast if the next entry is not valid XML.
                aggregateXMLSplitter.hasNext();
                return true;
            } catch (Exception ex) {
                if (fileContext.isReadAbortOnFailure()) {
                    throw ex;
                }
                aggregateXMLSplitter = null;
                Util.MAIN_LOGGER.warn(ex.getMessage());
            }
        }
    }

    /**
     * Find the next row to return, where a row is constructed from a child XML element as specified by the user-defined
     * aggregate XML element name and optional namespace. The most likely reason this will fail is because a
     * child element is found but it does not have the user-defined URI element in it.
     *
     * @return
     */
    private boolean findNextRowToReturn() {
        while (true) {
            // This hasNext() call shouldn't fail except when the splitter is first created, and we call it then to
            // ensure that the entry is a valid XML file.
            if (!aggregateXMLSplitter.hasNext()) {
                aggregateXMLSplitter = null;
                return false;
            }

            try {
                this.rowToReturn = this.aggregateXMLSplitter.nextRow(this.path + "-" + entryCounter);
                return true;
            } catch (Exception ex) {
                if (fileContext.isReadAbortOnFailure()) {
                    throw ex;
                }
                // Warn that the element failed, and keep going.
                Util.MAIN_LOGGER.warn(ex.getMessage());
            }
        }
    }
}
