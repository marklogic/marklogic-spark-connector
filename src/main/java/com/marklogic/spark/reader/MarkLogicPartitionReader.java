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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.catalyst.json.JacksonParser;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.HashMap;
import scala.compat.java8.JFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

class MarkLogicPartitionReader implements PartitionReader {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicPartitionReader.class);

    private final ReadContext readContext;
    private final PlanAnalysis.Partition partition;
    private final List<HostUtil.DataHost> dataHosts;

    private final JacksonParser jacksonParser;
    private final Function2<JsonFactory, String, JsonParser> jsonParserCreator;
    private final Function1<String, UTF8String> utf8StringCreator;

    private Iterator<StringHandle> rowIterator;
    private int nextBucketIndex;
    private int currentBucketRowCount;
    private int currentHostIndex = 0;


    MarkLogicPartitionReader(ReadContext readContext, PlanAnalysis.Partition partition, List<HostUtil.DataHost> dataHosts) {
        this.readContext = readContext;
        this.partition = partition;
        this.jacksonParser = newJacksonParser(readContext.getSchema());
        this.dataHosts = dataHosts;

        // To vary the order in which hosts are invoked across readers, select a random index in the host list to use
        // as the starting point.
        this.currentHostIndex = new Random().nextInt(dataHosts.size());

        // Used https://github.com/scala/scala-java8-compat in the DHF Spark 2 connector. Per the README for
        // scala-java8-compat, we should be able to use scala.jdk.FunctionConverters since those are part of Scala
        // 2.13. However, that is not yet working within PySpark. So sticking with this "legacy" appraoch as it seems
        // to work fine in both vanilla Spark (i.e. JUnit tests) and PySpark.
        this.jsonParserCreator = JFunction.func((jsonFactory, someString) -> CreateJacksonParser.string(jsonFactory, someString));
        this.utf8StringCreator = JFunction.func((someString) -> UTF8String.fromString(someString));
    }

    @Override
    public boolean next() {
        if (rowIterator != null) {
            if (rowIterator.hasNext()) {
                return true;
            } else {
                logger.debug("Count of rows for partition {} and bucket {}: {}", this.partition,
                    this.partition.buckets.get(nextBucketIndex - 1), currentBucketRowCount);
                currentBucketRowCount = 0;
            }
        }

        // Iterate through buckets until we find one with at least one row.
        while (true) {
            boolean noBucketsLeftToQuery = nextBucketIndex == partition.buckets.size();
            if (noBucketsLeftToQuery) {
                return false;
            }

            PlanAnalysis.Bucket bucket = partition.buckets.get(nextBucketIndex);
            nextBucketIndex++;

            RowManager rowManager = selectRowManagerFromDataHosts();
            this.rowIterator = readContext.readRowsInBucket(rowManager, partition, bucket);
            boolean bucketHasAtLeastOneRow = this.rowIterator.hasNext();
            if (bucketHasAtLeastOneRow) {
                return true;
            }
        }
    }

    @Override
    public InternalRow get() {
        this.currentBucketRowCount++;
        String row = rowIterator.next().get();
        return this.jacksonParser.parse(row, this.jsonParserCreator, this.utf8StringCreator).head();
    }

    @Override
    public void close() {
    }

    /**
     * Spark's JacksonParser class is a critical part of our connector, though there doesn't seem to be much in the
     * way of public docs for it. Source code for it can be found at:
     * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/json/JacksonParser.scala
     * <p>
     * As noted in the code, JacksonParser can translate a string of JSON into a Spark InternalRow based on a schema.
     * That's exactly what we want, so we don't need to have any knowledge of how to convert to Spark's set of data
     * types.
     *
     * @param schema
     */
    private JacksonParser newJacksonParser(StructType schema) {
        // We don't expect corrupted records - i.e. corrupted values - to be present in the index. But Spark
        // requires that this be set. See https://medium.com/@sasidharan-r/how-to-handle-corrupt-or-bad-record-in-apache-spark-custom-logic-pyspark-aws-430ddec9bb41
        // for more information.
        final String defaultColumnNameOfCorruptRecord = "_corrupt_record";

        // TODO This may need to be configurable by the user
        final String defaultTimeZoneId = "Z";
        JSONOptions jsonOptions = new JSONOptions(new HashMap<>(), defaultTimeZoneId, defaultColumnNameOfCorruptRecord);

        // Have not yet found documentation on this parameter for JacksonParser, but it does not seem relevant as a
        // column value in TDE will be a single value and not an array.
        final boolean allowArraysAsStructs = true;

        // No use cases for filters so far.
        final Seq<Filter> filters = JavaConverters.asScalaIterator(new ArrayList().iterator()).toSeq();
        return new JacksonParser(schema, jsonOptions, allowArraysAsStructs, filters);
    }

    private RowManager selectRowManagerFromDataHosts() {
        int hostIndex = currentHostIndex % dataHosts.size();
        HostUtil.DataHost dataHost = dataHosts.get(hostIndex);
        if (logger.isDebugEnabled()) {
            logger.debug("Will send request to host: {}", dataHost.hostName);
        }
        currentHostIndex++;
        return dataHost.rowManager;
    }
}
