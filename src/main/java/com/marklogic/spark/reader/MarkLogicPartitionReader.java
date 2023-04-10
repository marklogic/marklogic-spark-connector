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

package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;


public class MarkLogicPartitionReader implements PartitionReader {

    private int index;
    private Iterator<RowRecord> rowRecordIterator;
    private Function<Row, InternalRow> rowConverter;

    public MarkLogicPartitionReader(StructType schema, Map<String, String> map) {
        this.rowConverter = new MarkLogicRowToInternalRowFunction(schema);
        System.out.println("************** In MarkLogicPartitionReader");
        DatabaseClient db = DatabaseClientFactory.newClient(propertyName -> map.get(propertyName));
        RowManager rowMgr = db.newRowManager();
        RawPlan builtPlan = rowMgr.newRawQueryDSLPlan(new StringHandle(map.get("marklogic.opticDsl")));
        rowRecordIterator = rowMgr.resultRows(builtPlan).iterator();
    }

    @Override
    public boolean next() {
        return rowRecordIterator.hasNext();
    }

    @Override
    public InternalRow get() {
        System.out.println("Calling get function");
        Row sparkRow = RowFactory.create(index, String.valueOf(rowRecordIterator.next()));
        index++;
        return this.rowConverter.apply(sparkRow);
    }

    @Override
    public void close() {
        System.out.println("Stopping");
    }
}
