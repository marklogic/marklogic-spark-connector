/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark;

import com.marklogic.spark.reader.CustomCodeScanBuilder;
import com.marklogic.spark.reader.OpticScanBuilder;
import com.marklogic.spark.reader.ReadContext;
import com.marklogic.spark.writer.MarkLogicWriteBuilder;
import com.marklogic.spark.writer.WriteContext;
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

public class MarkLogicTable implements SupportsRead, SupportsWrite {

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
        if (Util.hasOption(readProperties, Options.READ_INVOKE, Options.READ_JAVASCRIPT, Options.READ_XQUERY)) {
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
        return new OpticScanBuilder(new ReadContext(readProperties, readSchema));
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
    @SuppressWarnings("java:S1133")
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
