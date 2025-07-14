/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.DocumentPipeline;
import com.marklogic.spark.core.DocumentPipelineFactory;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.TripleRowSchema;
import com.marklogic.spark.writer.document.DocumentRowConverter;
import com.marklogic.spark.writer.file.ZipFileWriter;
import com.marklogic.spark.writer.rdf.RdfRowConverter;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
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

    private final DocumentPipeline documentPipeline;

    // Updated as batches are processed.
    private final AtomicInteger successItemCount = new AtomicInteger(0);
    private final AtomicInteger failedItemCount = new AtomicInteger(0);

    private final List<DocumentInputs> documentInputsBatch = new ArrayList<>();
    private final int pipelineBatchSize;

    WriteBatcherDataWriter(WriteContext writeContext, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.writeContext = writeContext;
        this.writeFailure = new AtomicReference<>();
        this.docBuilder = this.writeContext.newDocBuilder();
        this.databaseClient = writeContext.connectToMarkLogic();
        this.rowConverter = determineRowConverter();
        this.isStreamingFiles = writeContext.isStreamingFiles();
        this.documentManager = this.isStreamingFiles ? databaseClient.newDocumentManager() : null;
        this.documentPipeline = DocumentPipelineFactory.newDocumentPipeline(writeContext);
        this.pipelineBatchSize = writeContext.getIntOption(Options.WRITE_PIPELINE_BATCH_SIZE, 1, 1);

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
        Iterator<DocumentInputs> inputs = rowConverter.convertRow(row);
        buildDocumentsAndFlushAsNeeded(inputs);
    }

    @Override
    public WriterCommitMessage commit() {
        // The RDF row converter may have "pending" rows as it has not yet reached the max number of triples to include
        // in a document. Those are retrieved here.
        buildDocumentsAndFlushAsNeeded(rowConverter.getRemainingDocumentInputs());

        // May have a batch of documentInputs less than the pipeline batch size, so flush these.
        processDocumentInputsBatch();

        // Wait for the writeBatcher to finish all writes to MarkLogic.
        this.writeBatcher.flushAndWait();

        throwWriteFailureIfExists();

        Set<String> graphs = getGraphNames();
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
        IOUtils.closeQuietly(documentPipeline);
    }

    /**
     * "process" = run each set of inputs through the optional document pipeline if necessary. Then convert each
     * set of inputs into a set of documents and write those to MarkLogic.
     */
    private void processDocumentInputsBatch() {
        if (Util.MAIN_LOGGER.isDebugEnabled() && pipelineBatchSize > 1) {
            Util.MAIN_LOGGER.debug("Processing batch of documents, count: {}", documentInputsBatch.size());
        }
        if (documentPipeline != null) {
            documentPipeline.processDocuments(documentInputsBatch);
        }
        buildAndWriteDocuments(documentInputsBatch);
        documentInputsBatch.clear();
    }

    /**
     * Builds up a batch of document inputs based in the given iterator, which produces inputs based on a single row.
     * A row can return multiple instances of document inputs. If the size of the inputs batch is that of the pipeline
     * batch size or greater, flush the batch, which forces the inputs to be converted into real documents and written
     * to MarkLogic.
     *
     * @param iterator
     */
    private void buildDocumentsAndFlushAsNeeded(Iterator<DocumentInputs> iterator) {
        try {
            iterator.forEachRemaining(documentInputs -> {
                documentInputsBatch.add(documentInputs);
                if (documentInputsBatch.size() >= pipelineBatchSize) {
                    processDocumentInputsBatch();
                }
            });
        } finally {
            // This is needed for when files are being streamed into MarkLogic; gives a chance for the file reader to
            // close the associated InputStream.
            if (iterator instanceof Closeable) {
                IOUtils.closeQuietly((Closeable) iterator);
            }
        }
    }

    /**
     * Each {@code DocumentInputs} object can produce many documents to write to MarkLogic.
     *
     * @param list
     */
    private void buildAndWriteDocuments(List<DocumentInputs> list) {
        for (DocumentInputs inputs : list) {
            Collection<DocumentWriteOperation> documents = this.docBuilder.buildDocuments(inputs);
            for (DocumentWriteOperation document : documents) {
                if (this.isStreamingFiles) {
                    writeDocumentViaPutOperation(document);
                } else {
                    this.writeBatcher.add(document);
                }
            }
        }
    }

    /**
     * This provides a mechanism for capturing the list of graph names detected while processing RDF rows. These need
     * to be sent back to MarkLogicWrite, where each graph is written to MarkLogic as a graph document.
     *
     * @return
     */
    private Set<String> getGraphNames() {
        return this.rowConverter instanceof RdfRowConverter ?
            ((RdfRowConverter) rowConverter).getGraphs() :
            null;
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
        }
        final StructType schema = writeContext.getSchema();
        if (DocumentRowSchema.hasDocumentFields(schema)) {
            return new DocumentRowConverter(writeContext);
        } else if (TripleRowSchema.SCHEMA.equals(schema)) {
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
            // Originally, only the failure message was included under the impression that the Spark environment was
            // logging the full stacktrace. That either was not the case or is no longer the case on Spark 3.5.x.
            // So the original exception is retained. But oddly, this results in a SparkException with a null cause - ???.
            // That doesn't really impact a user - it's a SparkException regardless - but caused some tests to no longer
            // be able to catch a ConnectorException.
            throw new ConnectorException(failure.getMessage(), failure);
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
        final String stringContent = HandleAccessor.contentAsString(contentHandle);
        Objects.requireNonNull(stringContent);
        byte[] content = ByteArray.concat(stringContent.getBytes());

        GenericInternalRow row = new DocumentRowBuilder(new ArrayList<>())
            .withUri(failedDoc.getUri()).withContent(content)
            .withMetadata((DocumentMetadataHandle) failedDoc.getMetadata())
            .buildRow();

        try {
            archiveWriter.write(row);
        } catch (Exception e) {
            ConnectorException ex = new ConnectorException(String.format(
                "Unable to write failed documents to archive file at %s; URI of failed document: %s; cause: %s",
                archiveWriter.getZipFilePath(), failedDoc.getUri(), e.getMessage()
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
                Util.MAIN_LOGGER.info("Wrote failed documents to archive file at {}.", archiveWriter.getZipFilePath());
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
            writeContext.logBatchOnSuccess(1, 0);
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
