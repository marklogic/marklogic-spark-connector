package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Serializable;
import java.util.Map;

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

    FSDataInputStream open(FilePartition filePartition) {
        try {
            Path hadoopPath = new Path(filePartition.getPath());
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            return fileSystem.open(hadoopPath);
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePartition, e.getMessage()), e);
        }
    }
}
