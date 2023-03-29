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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;


public class MarkLogicPartitionReader implements PartitionReader {
    int index;
    Map<String, String> map;
    StructType schema;

    public MarkLogicPartitionReader(StructType schema,Map<String, String> map) {
        this.index = 0;
        this.map = map;
        this.schema = schema;
        System.out.println("************** In MarkLogicPartitionReader");

    }

    @Override
    public boolean next() {
        return index<3;
    }

    @Override
    public InternalRow get(){
        System.out.println("Calling get function");

        try {
            Row fakeRow = RowFactory.create(index, "doc"+index);
            index++;
            MarkLogicRowToInternalRowFunction markLogicRowToInternalRowFunction = new MarkLogicRowToInternalRowFunction(schema);
            return markLogicRowToInternalRowFunction.apply(fakeRow);
        } catch (Exception e) {
            throw e;

        }
    }

    @Override
    public void close() throws IOException {
        System.out.println("Stopping");
    }
}
