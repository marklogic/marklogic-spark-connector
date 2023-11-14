package com.marklogic.spark.reader;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class CustomCodeMicroBatchStream implements MicroBatchStream {

    private final static Logger logger = LoggerFactory.getLogger(CustomCodeMicroBatchStream.class);

    private final CustomCodeContext customCodeContext;
    private final List<String> partitions;
    private long partitionIndex = 0;

    CustomCodeMicroBatchStream(CustomCodeContext customCodeContext, List<String> partitions) {
        this.customCodeContext = customCodeContext;
        this.partitions = partitions;
    }

    /**
     * Invoked by Spark to get the next offset for which it should construct a reader; an offset for this class is
     * equivalent to a user-defined partition.
     *
     * @return
     */
    @Override
    public Offset latestOffset() {
        Offset result = partitionIndex >= partitions.size() ? null : new LongOffset(partitionIndex);
        if (logger.isTraceEnabled()) {
            logger.trace("Returning latest offset: {}", partitionIndex);
        }
        partitionIndex++;
        return result;
    }

    /**
     * @param start
     * @param end
     * @return a partition associated with the latest partition, which is captured by the "end" offset.
     */
    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        long index = ((LongOffset) end).offset();
        return new InputPartition[]{new CustomCodePartition(partitions.get((int) index))};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomCodePartitionReaderFactory(this.customCodeContext);
    }

    @Override
    public Offset initialOffset() {
        return new LongOffset(0);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return new LongOffset(Long.parseLong(json));
    }

    @Override
    public void commit(Offset end) {
        if (logger.isDebugEnabled()) {
            logger.debug("Committing offset: {}", end);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping");
    }
}
