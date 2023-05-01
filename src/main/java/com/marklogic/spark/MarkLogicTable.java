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
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MarkLogicTable implements SupportsRead, SupportsWrite {

    private ReadContext readContext;
    private Set<TableCapability> capabilities;
    private StructType schema;
    Map<String, String> properties;

    MarkLogicTable(ReadContext readContext) {
        this.readContext = readContext;
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        this.schema = readContext.getSchema();
    }

    public MarkLogicTable(StructType schema, Set<TableCapability> capabilities, Map<String, String> properties) {
        this.schema = schema;
        this.capabilities = capabilities;
        this.properties = properties;
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
    public String name() {
        // TODO Figure out a good name
        return "test-project";
    }

    // This is marked as deprecated in the Table interface.
    @Deprecated
    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        // TODO: Need to add capabilities accordingly for Stream write.
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
            capabilities.add(TableCapability.BATCH_WRITE);
        }
        return capabilities;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        // TODO: Look into the uses of LogicalWriteInfo
        return new MarkLogicWriteBuilder(properties);
    }
}
