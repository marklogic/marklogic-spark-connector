package com.marklogic.spark;

import com.marklogic.spark.reader.file.FileScanBuilder;
import com.marklogic.spark.writer.file.DocumentFileWriteBuilder;
import org.apache.hadoop.fs.FileStatus;
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

    MarkLogicFileTable(SparkSession sparkSession, CaseInsensitiveStringMap options, Seq<String> paths, StructType schema) {
        super(sparkSession, options, paths, Option.apply(schema));
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new FileScanBuilder(super.fileIndex());
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        // Need to pass along a serializable object.
        return new DocumentFileWriteBuilder(this.options.asCaseSensitiveMap());
    }

    @Override
    public Option<StructType> inferSchema(Seq<FileStatus> files) {
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
}
