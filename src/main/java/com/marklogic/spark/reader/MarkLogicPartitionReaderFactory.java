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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class MarkLogicPartitionReaderFactory implements PartitionReaderFactory {

    final static long serialVersionUID = 1;

    private final Logger logger = LoggerFactory.getLogger(MarkLogicPartitionReaderFactory.class);
    private final ReadContext readContext;

    MarkLogicPartitionReaderFactory(ReadContext readContext) {
        this.readContext = readContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        logger.info("Creating reader for partition: {}", partition);
        List<HostUtil.DataHost> dataHosts = determineDataHosts();
        return new MarkLogicPartitionReader(this.readContext, (PlanAnalysis.Partition) partition, dataHosts);
    }

    private List<HostUtil.DataHost> determineDataHosts() {
        List<HostUtil.DataHost> dataHosts;
        DatabaseClient client = readContext.connectToMarkLogic();
        if (DatabaseClient.ConnectionType.GATEWAY.equals(client.getConnectionType())) {
            dataHosts = Arrays.asList(new HostUtil.DataHost(client));
        } else {
            dataHosts = HostUtil.getDataHosts(client);
            List<String> hostNames = dataHosts.stream().map(host -> host.hostName).collect(Collectors.toList());
            if (logger.isDebugEnabled()) {
                logger.debug("Will connect to hosts: {}", hostNames);
            }
        }
        return dataHosts;
    }
}
