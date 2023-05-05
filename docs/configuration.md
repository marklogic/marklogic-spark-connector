---
layout: default
title: Configuration Reference
nav_order: 6
---

The MarkLogic Spark connector has 3 sets of configuration options - connection options, reading options, and writing 
options. Each set of options is defined in a separate table below.

# Connection options

These options define how the connector connects and authenticates with MarkLogic.

| Option                                      | Description |
|---------------------------------------------| --- |
| spark.marklogic.client.host                 | Required; the host name to connect to; this can be the name of a host in your MarkLogic cluster or the host name of a load balancer. |
| spark.marklogic.client.port                 | Required; the port of the app server in MarkLogic to connect to. |
| spark.marklogic.client.basePath             | Base path to prefix on each request to MarkLogic. |
| spark.marklogic.client.database             | Name of the database to interact with; only needs to be set if it differs from the content database assigned to the app server that the connector will connect to.|
| spark.marklogic.client.connectionType       | Either `gateway` for when connecting to a load balancer, or `direct` (default). |
| spark.marklogic.client.authType             | Required; one of `basic`, `digest`, `cloud`, `kerberos`, `certificate`, or `saml`. |
| spark.marklogic.client.username             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.password             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.certificate.file     | Required for `certificate` authentication; the path to a certificate file. |
| spark.marklogic.client.certificate.password | Required for `certificate` authentication; the password for accessing the certificate file. |
| spark.marklogic.client.cloud.apiKey         | Required for MarkLogic `cloud` authentication. |
| spark.marklogic.client.kerberos.principal | Required for `kerberos` authentication. |
| spark.marklogic.client.saml.token | Required for `saml` authentication. |
| spark.marklogic.client.sslProtocol | If `default`, an SSL connection is created using the JVM's default SSL context; else the value is passed to the [SSLContext method](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html#getInstance-java.lang.String-) for instantiating an SSL context. |
| spark.marklogic.client.sslHostnameVerifier | Either `any`, `common`, or `strict`. |

# Read options

These options control how the connector reads data from MarkLogic. See [the guide on reading](reading.md) for more 
information on how data is read from MarkLogic.

| Option | Description                                                                                       | 
| --- |---------------------------------------------------------------------------------------------------|
| spark.marklogic.read.opticDsl | Required; the Optic DSL query to run for retrieving rows; must use `op.fromView` as the accessor. |
| spark.marklogic.read.numPartitions | The number of Spark partitions to create; defaults to `spark.default.parallelism` .               |
| spark.marklogic.read.batchSize | Approximate number of rows to retrieve in each call to MarkLogic; defaults to 10000.              |

## Schema support

# Write options

These options control how the connector writes data to MarkLogic. See [the guide on writing](writing.md) for more 
information on how data is written to MarkLogic.

| Option | Description                                                                                       | 
| --- |---------------------------------------------------------------------------------------------------|
| spark.marklogic.write.batchSize | The number of documents written in a call to MarkLogic; defaults to 100. |
| spark.marklogic.write.threadCount | The number of threads used within each partition to send documents to MarkLogic; defaults to 4. |

