/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.util.SerializableConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class FileBatch implements Batch {

    private final Map<String, String> properties;
    private final PartitioningAwareFileIndex fileIndex;

    FileBatch(Map<String, String> properties, PartitioningAwareFileIndex fileIndex) {
        this.properties = properties;
        this.fileIndex = fileIndex;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<String> filePaths = new ArrayList<>();
        // Need to use allFiles and not inputFiles; the latter surprisingly URL-encodes each file path.
        // Would likely be better to soon refactor the FilePartition class to hold a FileStatus instead of a String so
        // that we don't need to convert it at all.
        fileIndex.allFiles().iterator().foreach(fileStatus -> filePaths.add(fileStatus.getPath().toString()));

        int numPartitions = getNumberOfPartitions(filePaths);
        return FileUtil.makeFilePartitions(filePaths.toArray(new String[0]), numPartitions);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        // This config is needed to resolve file paths. This is our last chance to access it and provide a serialized
        // version to the factory, which must be serializable itself.
        Configuration config = SparkSession.active().sparkContext().hadoopConfiguration();
        FileContext fileContext = new FileContext(properties, new SerializableConfiguration(config));
        return new FilePartitionReaderFactory(fileContext);
    }

    private int getNumberOfPartitions(List<String> filePaths) {
        if (properties.containsKey(Options.READ_NUM_PARTITIONS)) {
            String value = properties.get(Options.READ_NUM_PARTITIONS);
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new ConnectorException(String.format("Invalid value for number of partitions: %s", value));
            }
        }
        return filePaths.size();
    }
}
