package com.marklogic.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Interprets a "micro batch" as a bucket. This gives the user control over how many micro batches will be created, as
 * the user can adjust the number of partitions and the batch size to affect how many buckets are created.
 * <p>
 * Within the scope of this class, an offset is equivalent to an index in the list of all buckets across all partitions
 * present in the {@code PlanAnalysis}. Each bucket is defined by lower/upper row ID bounds. So to refer to a bucket,
 * we simply need to know the index of the bucket in the list of all buckets. And thus, an offset is simply the index of
 * a bucket in that list.
 */
class MarkLogicMicroBatchStream implements MicroBatchStream {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicMicroBatchStream.class);

    private ReadContext readContext;
    private List<PlanAnalysis.Bucket> allBuckets;
    private int bucketIndex;

    MarkLogicMicroBatchStream(ReadContext readContext) {
        this.readContext = readContext;
        this.allBuckets = this.readContext.getPlanAnalysis().getAllBuckets();
    }

    @Override
    public Offset latestOffset() {
        if (bucketIndex >= this.allBuckets.size()) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Returning latest offset: {}", bucketIndex);
        }
        return new LongOffset(bucketIndex++);
    }

    /**
     * The offset is treated as the index of a bucket in the list of buckets. Thus, the concept of "start" and "end"
     * isn't relevant here - just the "end" offset is needed, which identifies the next bucket to process.
     *
     * @param start
     * @param end
     * @return
     */
    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        int index = (int) ((LongOffset) end).offset();
        return index >= allBuckets.size() ?
            null :
            new InputPartition[]{new PlanAnalysis.Partition(index, allBuckets.get(index))};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new MarkLogicPartitionReaderFactory(this.readContext);
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
