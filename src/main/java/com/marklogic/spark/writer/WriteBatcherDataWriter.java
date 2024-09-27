/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.GenericWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.TripleRowSchema;
import com.marklogic.spark.writer.file.ZipFileWriter;
import com.marklogic.spark.writer.rdf.RdfRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Uses the Java Client's WriteBatcher to handle writing rows as documents to MarkLogic.
 */
class WriteBatcherDataWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(WriteBatcherDataWriter.class);

    private final WriteContext writeContext;
    private final DatabaseClient databaseClient;
    private final DataMovementManager dataMovementManager;
    private final WriteBatcher writeBatcher;
    private final BatchRetrier batchRetrier;
    private final ZipFileWriter archiveWriter;

    private final DocBuilder docBuilder;

    // Used to capture the first failure that occurs during a request to MarkLogic.
    private final AtomicReference<Throwable> writeFailure;

    private final RowConverter rowConverter;

    private final boolean isStreamingFiles;
    // Only initialized if streaming files.
    private final GenericDocumentManager documentManager;

    // Updated as batches are processed.
    private final AtomicInteger successItemCount = new AtomicInteger(0);
    private final AtomicInteger failedItemCount = new AtomicInteger(0);

    WriteBatcherDataWriter(WriteContext writeContext, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.writeContext = writeContext;
        this.writeFailure = new AtomicReference<>();
        this.docBuilder = this.writeContext.newDocBuilder();
        this.databaseClient = writeContext.connectToMarkLogic();
        this.rowConverter = determineRowConverter();
        this.isStreamingFiles = writeContext.isStreamingFiles();
        this.documentManager = this.isStreamingFiles ? databaseClient.newDocumentManager() : null;

        if (writeContext.isAbortOnFailure()) {
            this.batchRetrier = null;
            this.archiveWriter = null;
        } else {
            this.batchRetrier = makeBatchRetrier();
            this.archiveWriter = writeContext.hasOption(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS) ?
                createArchiveWriter(hadoopConfiguration, partitionId) : null;
        }

        this.dataMovementManager = this.databaseClient.newDataMovementManager();
        this.writeBatcher = writeContext.newWriteBatcher(this.dataMovementManager);
        addBatchListeners(this.writeBatcher);
        this.dataMovementManager.startJob(this.writeBatcher);
    }

    @Override
    public void write(InternalRow row) {
        throwWriteFailureIfExists();

        Iterator<DocBuilder.DocumentInputs> iterator = rowConverter.convertRow(row);
        while (iterator.hasNext()) {
            DocumentWriteOperation writeOp = this.docBuilder.build(iterator.next());
            if (this.isStreamingFiles) {
                writeDocumentViaPutOperation(writeOp);
            } else {
                this.writeBatcher.add(writeOp);
            }
        }
    }

    @Override
    public WriterCommitMessage commit() {
        List<DocBuilder.DocumentInputs> documentInputs = rowConverter.getRemainingDocumentInputs();
        if (documentInputs != null) {
            documentInputs.forEach(inputs -> {
                DocumentWriteOperation writeOp = this.docBuilder.build(inputs);
                this.writeBatcher.add(writeOp);
            });
        }
        this.writeBatcher.flushAndWait();

        throwWriteFailureIfExists();

        // Need this hack so that the complete set of graphs can be reported back to MarkLogicWrite, which handles
        // creating the graphs after all documents have been written.
        Set<String> graphs = null;
        if (this.rowConverter instanceof RdfRowConverter) {
            graphs = ((RdfRowConverter) rowConverter).getGraphs();
        }

        return new CommitMessage(successItemCount.get(), failedItemCount.get(), graphs);
    }

    @Override
    public void abort() {
        Util.MAIN_LOGGER.warn("Abort called.");
        stopJobAndRelease();
        closeArchiveWriter();
        Util.MAIN_LOGGER.info("Finished abort.");
    }

    @Override
    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug("Close called.");
        }
        stopJobAndRelease();
        closeArchiveWriter();
    }

    private void addBatchListeners(WriteBatcher writeBatcher) {
        writeBatcher.onBatchSuccess(batch -> this.successItemCount.getAndAdd(batch.getItems().length));
        if (writeContext.isAbortOnFailure()) {
            // WriteBatcherImpl has its own warn-level logging which is a bit verbose, including more than just the
            // message from the server. This is intended to always show up and be associated with our Spark connector
            // and also to be more brief, just capturing the main message from the server.
            writeBatcher.onBatchFailure((batch, failure) -> {
                Util.MAIN_LOGGER.error("Failed to write documents: {}", failure.getMessage());
                this.writeFailure.compareAndSet(null, failure);
            });
        } else {
            writeBatcher.onBatchFailure(this.batchRetrier::retryBatch);
        }
    }

    private RowConverter determineRowConverter() {
        if (writeContext.isUsingFileSchema()) {
            return new FileRowConverter(writeContext);
        } else if (DocumentRowSchema.SCHEMA.equals(writeContext.getSchema())) {
            return new DocumentRowConverter(writeContext);
        } else if (TripleRowSchema.SCHEMA.equals(writeContext.getSchema())) {
            return new RdfRowConverter(writeContext);
        }
        return new ArbitraryRowConverter(writeContext);
    }

    private synchronized void throwWriteFailureIfExists() {
        if (writeFailure.get() != null) {
            Throwable failure = writeFailure.get();
            if (failure instanceof ConnectorException) {
                throw (ConnectorException) failure;
            }
            // Only including the message seems sufficient here, as Spark is logging the stacktrace. And the user
            // most likely only needs to know the message.
            throw new ConnectorException(failure.getMessage());
        }
    }

    private void stopJobAndRelease() {
        if (this.writeBatcher != null && this.dataMovementManager != null) {
            this.dataMovementManager.stopJob(this.writeBatcher);
        }
        if (this.databaseClient != null) {
            this.databaseClient.release();
        }
    }

    private BatchRetrier makeBatchRetrier() {
        return new BatchRetrier(
            writeContext.newDocumentManager(this.databaseClient),
            writeContext.getStringOption(Options.WRITE_TEMPORAL_COLLECTION),
            successfulBatch -> successItemCount.getAndAdd(successfulBatch.size()),
            (failedDoc, failure) -> {
                captureFailure(failure.getMessage(), failedDoc.getUri());
                if (this.archiveWriter != null) {
                    writeFailedDocumentToArchive(failedDoc);
                }
            }
        );
    }

    /**
     * Need this to be synchronized so that 2 or more WriteBatcher threads don't try to write zip entries at the same
     * time.
     *
     * @param failedDoc
     */
    private synchronized void writeFailedDocumentToArchive(DocumentWriteOperation failedDoc) {
        AbstractWriteHandle contentHandle = failedDoc.getContent();
        byte[] content = ByteArray.concat(HandleAccessor.contentAsString(contentHandle).getBytes());

        GenericInternalRow row = new DocumentRowBuilder(new ArrayList<>())
            .withUri(failedDoc.getUri()).withContent(content)
            .withMetadata((DocumentMetadataHandle) failedDoc.getMetadata())
            .buildRow();

        try {
            archiveWriter.write(row);
        } catch (Exception e) {
            ConnectorException ex = new ConnectorException(String.format(
                "Unable to write failed documents to archive file at %s; URI of failed document: %s; cause: %s",
                archiveWriter.getZipPath(), failedDoc.getUri(), e.getMessage()
            ), e);
            this.writeFailure.compareAndSet(null, ex);
            throw ex;
        }
    }

    private ZipFileWriter createArchiveWriter(SerializableConfiguration hadoopConfiguration, int partitionId) {
        String path = writeContext.getStringOption(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS);
        // The zip file is expected to be created lazily - i.e. only when a document fails. This avoids creating
        // empty archive zip files when no errors occur.
        return new ZipFileWriter(path, writeContext.getProperties(), hadoopConfiguration, partitionId, false);
    }

    private void closeArchiveWriter() {
        if (archiveWriter != null) {
            if (failedItemCount.get() > 0) {
                Util.MAIN_LOGGER.info("Wrote failed documents to archive file at {}.", archiveWriter.getZipPath());
            }
            archiveWriter.close();
        }
    }

    /**
     * A user typically chooses to stream a document due to its size. A PUT call to v1/documents can handle a document
     * of any size. But a POST call seems to have a limitation due to the multipart nature of the request - the body
     * part appears to be read into memory, which can cause the server to run out of memory. So for streaming, a PUT
     * call is made, which means we don't use the WriteBatcher.
     *
     * @param writeOp
     */
    private void writeDocumentViaPutOperation(DocumentWriteOperation writeOp) {
        final String uri = replaceSpacesInUriForPutEndpoint(writeOp.getUri());
        try {
            this.documentManager.write(uri, writeOp.getMetadata(), (GenericWriteHandle) writeOp.getContent());
            this.successItemCount.incrementAndGet();
        } catch (RuntimeException ex) {
            captureFailure(ex.getMessage(), uri);
            this.writeFailure.compareAndSet(null, ex);
        }
    }

    /**
     * Sigh. Using URLEncoder.encode will convert forward slashes into "%2F", which a user almost certainly does not
     * want, since those are meaningful in MarkLogic URIs. The main problem to address with the PUT endpoint is that it
     * erroneously does not accept spaces (see MLE-17088). So this simply replaces spaces.
     */
    private String replaceSpacesInUriForPutEndpoint(String uri) {
        return uri.replace(" ", "%20");
    }

    private void captureFailure(String message, String documentUri) {
        Util.MAIN_LOGGER.error("Unable to write document with URI: {}; cause: {}", documentUri, message);
        failedItemCount.incrementAndGet();
    }
}
