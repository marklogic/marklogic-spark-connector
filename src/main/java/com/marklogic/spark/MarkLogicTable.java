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

import com.marklogic.spark.reader.MarkLogicScanBuilder;
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

import java.util.HashSet;
import java.util.Set;

public class MarkLogicTable implements SupportsRead, SupportsWrite {

    private static Set<TableCapability> capabilities;
    private ReadContext readContext;
    private WriteContext writeContext;

    static {
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.MICRO_BATCH_READ);
        capabilities.add(TableCapability.BATCH_WRITE);
        capabilities.add(TableCapability.STREAMING_WRITE);
    }

    MarkLogicTable(ReadContext readContext) {
        this.readContext = readContext;
    }

    MarkLogicTable(WriteContext writeContext) {
        this.writeContext = writeContext;
    }


    /**
     * We ignore the {@code options} map per the class's Javadocs, which note that it's intended to provide
     * options for v2 implementations which expect case-insensitive keys. The map of properties provided by the
     * {@code TableProvider} are sufficient for our connector.
     *
     * @param options The options for reading, which is an immutable case-insensitive
     *                string-to-string map.
     * @return
     */
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MarkLogicScanBuilder(readContext);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new MarkLogicWriteBuilder(writeContext);
    }

    @Override
    public String name() {
        // TODO Figure out a good name
        return "test-project";
    }

    // This is marked as deprecated in the Table interface.
    @Deprecated
    @Override
    public StructType schema() {
        return readContext != null ? readContext.getSchema() : writeContext.getSchema();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
