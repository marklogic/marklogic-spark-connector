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
package com.marklogic.spark.writer;

import com.marklogic.spark.MarkLogicTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
public class MarkLogicWriteDataSource implements TableProvider {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // TODO: Need a story to read the schema passed in.
       // return new StructType(options.get("spark.marklogic.client.schema"));
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        if(properties.containsKey("spark.marklogic.client.schema")){
            CaseInsensitiveStringMap caseInsensitiveStringMap = new CaseInsensitiveStringMap(properties);
            return new MarkLogicTable(inferSchema(caseInsensitiveStringMap), null, properties);
        }
        return new MarkLogicTable(schema, null, properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
