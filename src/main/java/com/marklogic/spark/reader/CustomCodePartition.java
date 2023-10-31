package com.marklogic.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * For now, we cannot create partitions based on user's custom code. Will enhance this in the future if/when we add
 * support for partitions based on host and/or forest names.
 */
class CustomCodePartition implements InputPartition, Serializable {
    final static long serialVersionUID = 1;
}
