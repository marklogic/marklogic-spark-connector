package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.zip.GZIPInputStream;

class FileContext extends ContextSupport implements Serializable {

    private SerializableConfiguration hadoopConfiguration;

    FileContext(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        super(properties);
        this.hadoopConfiguration = hadoopConfiguration;
    }

    boolean isZip() {
        return "zip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    boolean isGzip() {
        return "gzip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    InputStream open(FilePartition filePartition) {
        try {
            Path hadoopPath = new Path(filePartition.getPath());
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            FSDataInputStream inputStream = fileSystem.open(hadoopPath);
            return this.isGzip() ? new GZIPInputStream(inputStream) : inputStream;
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePartition, e.getMessage()), e);
        }
    }

    boolean isReadAbortOnFailure() {
        if (hasOption(Options.READ_FILES_ABORT_ON_FAILURE)) {
            return Boolean.parseBoolean(getStringOption(Options.READ_FILES_ABORT_ON_FAILURE));
        }
        return true;
    }
}
