---
layout: default
title: Reading rows via custom code
parent: Reading Data
nav_order: 2
---

Rows can be retrieved via custom code executed by MarkLogic and written in either JavaScript or XQuery. This can be
useful when you need to retrieve data that cannot be easily accessed via Optic, or you are looking for behavior similar
to that of [MarkLogic's CoRB tool](https://github.com/marklogic-community/corb2) for processing data already in
MarkLogic.

When using this feature, please ensure that your MarkLogic user has the required privileges for the
MarkLogic REST [eval endpoint](https://docs.marklogic.com/REST/POST/v1/eval) and
[invoke endpoint](https://docs.marklogic.com/REST/POST/v1/invoke).

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Usage

Custom code can be [written in JavaScript](https://docs.marklogic.com/guide/getting-started/javascript) by
configuring the `spark.marklogic.read.javascript` option:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, null, cts.collectionQuery('employee'))") \
    .load()
df.show()
```

Or code can be [written in XQuery](https://docs.marklogic.com/guide/getting-started/XQueryTutorial) by configuring the
`spark.marklogic.read.xquery` option:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.xquery", "cts:uris((), (), cts:collection-query('employee'))") \
    .load()
df.show()
```

You can also invoke a JavaScript or XQuery module in your application's modules database via the
`spark.marklogic.read.invoke` option:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.invoke", "/read.sjs") \
    .load()
df.show()
```

## Custom code schemas

While the connector can infer a schema when executing an Optic query, it does not have any way to do so with custom
code, which can return any kind of data. As a sensible default - particularly since a common use case for
executing custom code is to return one or more documents URIs - the connector will assume a schema with a single
column named "URI" and of type string. The custom code is thus expected to return a sequence of zero or more values,
each of which will become a string value in a row with a single column named "URI".

If your custom code needs to return a different sequence of values, you can specify the schema in your Spark program
and the connector will use it instead. For example, the following expects that the `/path/to/module.sjs` will return
JSON objects with columns that conform to the given schema:

```
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.invoke", "/read-custom-schema.sjs") \
    .schema(StructType([StructField("id", IntegerType()), StructField("name", StringType())])) \
    .load()
df.show()
```

## Custom external variables

You can pass external variables to your custom code by configuring one or more options with names starting with
`spark.marklogic.read.vars.`. The remainder of the option name will be used as the external variable name, and the value
of the option will be sent as the external variable value. Each external variable will be passed as a string due to
Spark capturing all option values as strings.

The following demonstrates two custom external variables being configured and used by custom JavaScript code:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.vars.var1", "Engineering") \
    .option("spark.marklogic.read.vars.var2", "Marketing") \
    .option("spark.marklogic.read.javascript", "var var1, var2; cts.uris(null, null, cts.wordQuery([var1, var2]))") \
    .load()
df.show()
```

## Defining partitions for custom code

By default, the connector will send a single request to MarkLogic to execute custom code for reading rows. If your
custom code returns a large amount of data and is at risk of timing out, or if you seek better performance by breaking
your query into many smaller queries, you can use one of the following options to define partitions for your custom code:

- `spark.marklogic.read.partitions.invoke`
- `spark.marklogic.read.partitions.javascript`
- `spark.marklogic.read.partitions.xquery`

If one of the above options is defined, the connector will execute the code associated with the option and expect a
sequence of values to be returned. You can return any values you want to define partitions; the connector does not care
what the values represent. The connector will then execute your custom code - defined by `spark.marklogic.read.invoke`,
`spark.marklogic.read.javascript`, or `spark.marklogic.read.xquery` - once for each partition value. The partition value
will be defined in an external variable named `PARTITION`. Note as well that any external variables you define via the
`spark.marklogic.read.vars` prefix will also be sent to the code for returning partitions.

The following example shows a common use case for using MarkLogic forest IDs as partitions:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.partitions.javascript", "xdmp.databaseForests(xdmp.database())") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, null, cts.collectionQuery('employee'), 0, [PARTITION])") \
    .load()
df.show()
```

In the example application used by this documentation the "spark-example-content" database has 3 forests. Thus, the
partitions code above will return a sequence of 3 forest IDs. The connector will then invoke the custom
JavaScript code 3 times, once for each forest ID, with the `PARTITION` variable populated with a forest ID.

For the above scenario, it is common to run these queries
[at the same point in time](https://docs.marklogic.com/guide/app-dev/point_in_time). Because you are free to return
any partition values you wish, one technique for this scenario would be to construct partitions containing both a
forest ID and a server timestamp:

```
const forestIds = xdmp.databaseForests(xdmp.database())
const timestamp = xdmp.requestTimestamp()
Sequence.from(forestIds.toArray().map(forestId => forestId + ":" + timestamp))
```

In the custom code for returning rows, you can then obtain both a forest ID and a server timestamp from the partition
value and use them to ensure each of your queries runs at the same point in time.

## Streaming support

Just like for reading rows with Optic, the connector supports
[streaming reads](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
from MarkLogic via micro-batches. The connector configuration does not change; instead, different Spark APIs are used
to read a stream of data from MarkLogic. This can be useful for when you wish to obtain a batch of results from
MarkLogic and immediately send them to a Spark writer.

When streaming results from your custom code, you will need to set one of the options described above - either
`spark.marklogic.read.partitions.invoke`, `spark.marklogic.read.partitions.javascript`, or
`spark.marklogic.read.partitions.xquery` - for defining partitions.

The following example shows how the same connector configuration can be used for defining partitions and the custom
code for returning rows, just with different Spark APIs. In this example, Spark will invoke the custom code once
for each partition, with the returned batch of rows being immediately streamed to the writer, which prints the
batch of rows to the console:

```
stream = spark.readStream \
    .format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.partitions.javascript", "xdmp.databaseForests(xdmp.database())") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, null, cts.collectionQuery('employee'), null, [PARTITION]);") \
    .load() \
    .writeStream \
    .format("console") \
    .start()
stream.processAllAvailable()
stream.stop()
```

## Tuning performance

A key difference with reading via custom code is that unless you are using Spark streaming, a single call will be made
to MarkLogic to execute the custom code. Therefore, the options `spark.marklogic.read.batchSize` and
`spark.marklogic.read.numPartitions` will not have any impact on the performance of executing custom code.
You must therefore ensure that your custom code can be expected to complete in a single call to MarkLogic without
timing out. The support for Spark streaming described above can help if your query is at risk of timing out, as you can
effectively break it up into many smaller queries. However, note that Spark streaming sends many datasets to the writer
instead of a single dataset; this may not be appropriate for your use case.
