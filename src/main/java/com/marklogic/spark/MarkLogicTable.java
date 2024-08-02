/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.spark.reader.optic.OpticScanBuilder;
import com.marklogic.spark.reader.optic.OpticReadContext;
import com.marklogic.spark.reader.customcode.CustomCodeScanBuilder;
import com.marklogic.spark.writer.MarkLogicWriteBuilder;
import com.marklogic.spark.writer.WriteContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class MarkLogicTable implements SupportsRead, SupportsWrite {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicTable.class);

    private static Set<TableCapability> capabilities;

    private StructType readSchema;
    private Map<String, String> readProperties;

    private WriteContext writeContext;

    static {
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.MICRO_BATCH_READ);
        capabilities.add(TableCapability.BATCH_WRITE);
        capabilities.add(TableCapability.STREAMING_WRITE);
    }

    MarkLogicTable(StructType schema, Map<String, String> properties) {
        this.readSchema = schema;
        this.readProperties = properties;
    }

    MarkLogicTable(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    /**
     * We ignore the {@code options} map per the class's Javadocs, which note that it's intended to provide
     * options for v2 implementations which expect case-insensitive keys. The map of properties provided by the
     * {@code TableProvider} are sufficient for our connector, particularly as those keys are case-sensitive, which is
     * expected by the Java Client's method for creating a client based on case-sensitive property names.
     *
     * @param options The options for reading, which is an immutable case-insensitive string-to-string map.
     * @return
     */
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        if (Util.isReadWithCustomCodeOperation(readProperties)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Will read rows via custom code");
            }
            return new CustomCodeScanBuilder(readProperties, readSchema);
        }

        // A ReadContext is created at this point, as a new ScanBuilder is created each time the user invokes a
        // function that requires a result, such as "count()".
        if (logger.isDebugEnabled()) {
            logger.debug("Will read rows via Optic query");
        }

        // This is needed by the Optic partition reader; capturing it in the ReadContext so that the reader does not
        // have a dependency on an active Spark session, which makes certain kinds of tests easier.
        int defaultMinPartitions = SparkSession.active().sparkContext().defaultMinPartitions();
        return new OpticScanBuilder(new OpticReadContext(readProperties, readSchema, defaultMinPartitions));
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new MarkLogicWriteBuilder(writeContext);
    }

    @Override
    public String name() {
        return "MarkLogicTable()";
    }

    /**
     * @deprecated Marked as deprecated in the Table interface.
     */
    @SuppressWarnings({"java:S1133", "java:S6355"})
    @Deprecated
    @Override
    public StructType schema() {
        return readSchema != null ? readSchema : writeContext.getSchema();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
