package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.CustomCodeContext;
import com.marklogic.spark.Options;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class CustomCodeMicroBatchStream implements MicroBatchStream {

    private final static Logger logger = LoggerFactory.getLogger(CustomCodeMicroBatchStream.class);

    private final CustomCodeContext customCodeContext;
    private long batchIndex = 0;
    private final List<String> batchIds = new ArrayList<>();

    /**
     * Invokes the user-defined option for retrieving batch IDs. The list of batch IDs is stored so that it can be
     * iterated over via the methods in MicroBatchStream.
     *
     * @param customCodeContext
     */
    CustomCodeMicroBatchStream(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
        DatabaseClient client = this.customCodeContext.connectToMarkLogic();
        try {
            this.customCodeContext
                .buildCall(client, new CustomCodeContext.CallOptions(
                    Options.READ_BATCH_IDS_INVOKE, Options.READ_BATCH_IDS_JAVASCRIPT, Options.READ_BATCH_IDS_XQUERY
                ))
                .eval()
                .forEach(result -> batchIds.add(result.getString()));
        } finally {
            client.release();
        }
    }

    /**
     * Invoked by Spark to get the next offset for which it should construct a reader; an offset for this class is
     * equivalent to a batch ID.
     *
     * @return
     */
    @Override
    public Offset latestOffset() {
        Offset result = batchIndex >= batchIds.size() ? null : new LongOffset(batchIndex);
        if (logger.isTraceEnabled()) {
            logger.trace("Returning latest offset: {}", batchIndex);
        }
        batchIndex++;
        return result;
    }

    /**
     * @param start
     * @param end
     * @return a partition associated with the latest batch ID, which is captured by the "end" offset.
     */
    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        long index = ((LongOffset) end).offset();
        return new InputPartition[]{new CustomCodePartition(batchIds.get((int) index))};
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
