/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.spark.reader.file.FileScanBuilder;
import com.marklogic.spark.writer.file.DocumentFileWriteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileTable;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.Seq;

/**
 * Extends Spark's FileTable class so that it can make use of that class's file index capabilities, which includes
 * support for Spark options like recursiveFileLookup and pathGlobFilter as defined at
 * https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html .
 * <p>
 * A prototype that attempted to create an InMemoryFileIndex, and thus avoid the need to subclass FileTable, was not
 * successful. Could not get the following code to run in Java:
 * Seq<Path> hadoopPaths = DataSource.checkAndGlobPathIfNecessary(this.paths,
 * session.sparkContext().hadoopConfiguration(), true, true, numThreads, true);
 * For future attempts, the example at https://stackoverflow.com/a/45373345/3306099 is useful for converting a Java
 * map into an immutable Scala map.
 */
class MarkLogicFileTable extends FileTable {

    private final CaseInsensitiveStringMap options;
    private final StructType schema;

    MarkLogicFileTable(SparkSession sparkSession, CaseInsensitiveStringMap options, scala.collection.immutable.Seq<String> paths, StructType schema) {
        super(sparkSession, options, paths, Option.apply(schema));
        if (isWriteFilesOperation(options, paths)) {
            makeWritePath(paths.head(), sparkSession);
        }
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        if ("true".equalsIgnoreCase(options.get(Options.STREAM_FILES)) && Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("File streaming is enabled; will read files during writer phase.");
        }
        return new FileScanBuilder(options.asCaseSensitiveMap(), super.fileIndex());
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        // Need to pass along a serializable object.
        return new DocumentFileWriteBuilder(this.options.asCaseSensitiveMap(), this.schema);
    }

    @Override
    public Option<StructType> inferSchema(scala.collection.immutable.Seq<FileStatus> files) {
        return Option.apply(this.schema);
    }

    @Override
    public String name() {
        return "marklogic-file";
    }

    @Override
    public String formatName() {
        // Per the docs in FileTable, this is providing an alias for supported file types. It does not appear to have
        // any impact on functionality.
        return "marklogic";
    }

    @Override
    public Class<? extends FileFormat> fallbackFileFormat() {
        // Per the docs in FileTable, this allows for returning a Spark V1 FileFormat. We don't have support for that,
        // so null is returned.
        return null;
    }

    private boolean isWriteFilesOperation(CaseInsensitiveStringMap options, Seq<String> paths) {
        // When writing files, a user is limited to a single path. So if the user provides multiple paths when
        // reading files, we immediately know it's not a write operation.
        if (paths.size() != 1) {
            return false;
        }
        return options.keySet().stream()
            // There are no required options to indicate whether this is a read or write. So we at least check for some
            // options that would indicate that the user is reading files, in which case we don't need to call mkdirs.
            .noneMatch(key -> key.startsWith("spark.marklogic.read.files")
                || key.startsWith("spark.marklogic.read.aggregates.xml")
            );
    }

    private void makeWritePath(String path, SparkSession sparkSession) {
        Configuration config = sparkSession.sparkContext().hadoopConfiguration();
        try {
            Path hadoopPath = new Path(path);
            FileSystem fileSystem = hadoopPath.getFileSystem(config);
            if (!fileSystem.exists(hadoopPath)) {
                if (Util.MAIN_LOGGER.isDebugEnabled()) {
                    Util.MAIN_LOGGER.debug("Calling mkdirs on path: {}", path);
                }
                fileSystem.mkdirs(hadoopPath);
            }
        } catch (Exception ex) {
            // We'll get an exception in 1 of 2 scenarios. First, this is a write operation and the mkdirs call
            // fails. In that scenario, the user will still get an AnalysisException from Spark noting that the path
            // does not exist. The user likely won't be able to make the path themselves - perhaps a permissions issue -
            // and thus has the info they need to proceed. In the other scenario, this is a read operation and the
            // mkdirs call didn't need to happen. In that scenario, the user will still get an AnalysisException because
            // the directory could not be found. So the user has enough information to proceed there as well. So this
            // is only being logged at the debug level as the AnalysisException should be sufficient for helping the
            // user to fix their problem.
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Unable to call mkdirs on path: {}; cause: {}", path, ex.getMessage());
            }
        }
    }
}
