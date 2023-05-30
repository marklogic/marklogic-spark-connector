---
layout: default
title: Configuration Reference
nav_order: 5
---

The MarkLogic Spark connector has 3 sets of configuration options - connection options, reading options, and writing 
options. Each set of options is defined in a separate table below.

## Connection options

These options define how the connector connects and authenticates with MarkLogic.

| Option                                      | Description |
|---------------------------------------------| --- |
| spark.marklogic.client.host                 | Required; the host name to connect to; this can be the name of a host in your MarkLogic cluster or the host name of a load balancer. |
| spark.marklogic.client.port                 | Required; the port of the app server in MarkLogic to connect to. |
| spark.marklogic.client.basePath             | Base path to prefix on each request to MarkLogic. |
| spark.marklogic.client.database             | Name of the database to interact with; only needs to be set if it differs from the content database assigned to the app server that the connector will connect to.|
| spark.marklogic.client.connectionType       | Either `gateway` for when connecting to a load balancer, or `direct` when connecting directly to MarkLogic. Defaults to `gateway` which works in either scenario. |
| spark.marklogic.client.authType             | Required; one of `basic`, `digest`, `cloud`, `kerberos`, `certificate`, or `saml`. Defaults to `digest`. |
| spark.marklogic.client.username             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.password             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.certificate.file     | Required for `certificate` authentication; the path to a certificate file. |
| spark.marklogic.client.certificate.password | Required for `certificate` authentication; the password for accessing the certificate file. |
| spark.marklogic.client.cloud.apiKey         | Required for MarkLogic `cloud` authentication. |
| spark.marklogic.client.kerberos.principal | Required for `kerberos` authentication. |
| spark.marklogic.client.saml.token | Required for `saml` authentication. |
| spark.marklogic.client.sslProtocol | If `default`, an SSL connection is created using the JVM's default SSL context; else the value is passed to the [SSLContext method](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html#getInstance-java.lang.String-) for instantiating an SSL context. |
| spark.marklogic.client.sslHostnameVerifier | Either `any`, `common`, or `strict`. |
| spark.marklogic.client.uri | Shortcut for setting the host, port, username, and password when using `basic` or `digest` authentication. See below for more information. |

### Connecting with a client URI

The `spark.marklogic.client.uri` is a convenience for the common case of using `basic` or `digest` authentication.
It allows you to specify username, password, host, and port via the following syntax:

    username:password@host:port

This avoids the need to set the individual options for the above 4 properties.  

You may also configure a database name via the following syntax:

    username:password@host:port/database

A database name is only needed when you wish to work with a database other than the one associated with the app server 
that the connector will connect to via the `port` value. 

Using this convenience can provide a much more succinct set of options - for example:

```
df = spark.read.format("com.marklogic.spark")\
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8020")\
    .option("spark.marklogic.read.opticDsl", "op.fromView('example', 'employee')")\
    .load()
```

Note that if the username or password contain either a `@` or a `:` character, you must first convert them using 
[percent encoding](https://developer.mozilla.org/en-US/docs/Glossary/percent-encoding) into the correct character 
triplet. For example, a password of `sp@r:k` must appear in the `spark.marklogic.client.uri` string as `sp%40r%3Ak`. 

## Read options

These options control how the connector reads data from MarkLogic. See [the guide on reading](reading.md) for more 
information on how data is read from MarkLogic.

| Option | Description                                                                                       | 
| --- |---------------------------------------------------------------------------------------------------|
| spark.marklogic.read.opticDsl | Required; the Optic DSL query to run for retrieving rows; must use `op.fromView` as the accessor. |
| spark.marklogic.read.numPartitions | The number of Spark partitions to create; defaults to `spark.default.parallelism` .               |
| spark.marklogic.read.batchSize | Approximate number of rows to retrieve in each call to MarkLogic; defaults to 10000.              |

## Write options

These options control how the connector writes data to MarkLogic. See [the guide on writing](writing.md) for more 
information on how data is written to MarkLogic.

| Option | Description                                                                       | 
| --- |-----------------------------------------------------------------------------------|
| spark.marklogic.write.batchSize | The number of documents written in a call to MarkLogic; defaults to 100. |
| spark.marklogic.write.threadCount | The number of threads used within each partition to send documents to MarkLogic; defaults to 4. |
| spark.marklogic.write.collections | Comma-delimited string of collection names to add to each document |
| spark.marklogic.write.permissions | Comma-delimited string of role names and capabilities to add to each document - e.g. role1,read,role2,update,role3,execute |
| spark.marklogic.write.temporalCollection | Name of a temporal collection to assign each document to |
| spark.marklogic.write.transform | Name of a REST transform to apply to each document |
| spark.marklogic.write.transformParams | Comma-delimited string of transform parameter names and values - e.g. param1,value1,param2,value2 |
| spark.marklogic.write.transformParamsDelimiter | Delimiter to use instead of a command for the `transformParams` option |
| spark.marklogic.write.uriPrefix | String to prepend to each document URI, where the URI defaults to a UUID |
| spark.marklogic.write.uriSuffix | String to append to each document URI, where the URI defaults to a UUID |
| spark.marklogic.write.uriTemplate | String defining a template for constructing each document URI. See [Writing data](writing.md) for more information. |

