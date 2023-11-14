---
layout: default
title: Reading Data
nav_order: 3
---


The MarkLogic connector allows for data to be retrieved from MarkLogic as rows either via an  
[Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) or via custom code written in JavaScript or XQuery.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Reading rows via Optic

As shown in the [Getting Started with PySpark guide](getting-started/pyspark.md), a read operation will define
how the connector should connect to MarkLogic, the MarkLogic Optic query to run (or a custom JavaScript or XQuery query; 
see the next section for more information), and zero or more other options:

```
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load()
```

As demonstrated above, `format`, `spark.marklogic.client.uri` (or the other `spark.marklogic.client` options
that can be used to define the connection details), and `spark.marklogic.read.opticQuery` are required. The 
sections below provide more detail about these and other options that can be set. 

Your Optic query can include any of the 
[different kinds of Optic operations](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_35559) that MarkLogic 
supports. For example, the following demonstrates how the 
[powerful MarkLogic CTS API](https://docs.marklogic.com/guide/search-dev/cts_query) can be easily used within an 
Optic query to constrain the returned rows based on a search query (note that the JavaScript CTS API must be used in 
the Optic query):

```
query = "op.fromView('example', 'employee').where(cts.wordQuery('Drive'))"

df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", query) \
    .load()
```

The `where` clause in the example above can include any of the query features supported by MarkLogic, such as 
[geospatial queries](https://docs.marklogic.com/guide/search-dev/geospatial), 
[wildcard queries](https://docs.marklogic.com/guide/search-dev/wildcard), and 
query expansion via [a thesaurus](https://docs.marklogic.com/guide/search-dev/thesaurus) or 
[spelling correction](https://docs.marklogic.com/guide/search-dev/spelling).

### Optic query requirements

As of the 2.0.0 release of the connector, the Optic query must use the 
[op.fromView](https://docs.marklogic.com/op.fromView) accessor function. Future releases of both the connector and 
MarkLogic will strive to relax this requirement. 

In addition, calls to `groupBy`, `orderBy`, `limit`, and `offset` should be performed via Spark instead of within 
the initial Optic query. A key benefit of Spark and the MarkLogic connector is the ability to execute the query in 
parallel via multiple Spark partitions. The aforementioned calls, if made in the Optic query, may not produce the 
expected results if more than one Spark partition is used or if more than one request is made to MarkLogic. The 
equivalent Spark operations should be called instead, or the connector should be configured to make a single request 
to MarkLogic. See the "Pushing down operations" and "Tuning performance" sections below for more information.

Finally, the query must adhere to the handful of limitations imposed by the  
[Optic Query DSL](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710). A good practice in validating a 
query is to run it in your [MarkLogic server's qconsole tool](https://docs.marklogic.com/guide/qconsole) in a buffer 
with a query type of "Optic DSL". 

### Schema inference

The connector will infer a Spark schema automatically based on the view identified by `op.fromView` in
the Optic query. Each column returned by your Optic query will be mapped to a Spark schema column with the 
same name and an appropriate type. 

You may override this feature and provide your own schema instead. The example below shows how a custom schema can 
be provided within PySpark; this assumes that you have deployed the application in the 
[Getting Started with PySpark](getting-started/pyspark.md) guide:

```
from pyspark.sql.types import StructField, StructType, StringType
df = spark.read.format("com.marklogic.spark") \
    .schema(StructType([StructField("example.employee.GivenName", StringType()), StructField("example.employee.Surname", StringType())])) \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load()
```

### Accessing documents 

While the connector requires that an Optic query use `op.fromView` as its accessor function, documents can still be
retrieved via the [Optic functions for joining documents](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_78437). 

For example, the following query will find all matching rows and then retrieve the documents and URIs associated with 
those rows:

```
query = "const joinCol = op.fragmentIdCol('id'); \
op.fromView('example', 'employee', '', joinCol) \
  .joinDoc('doc', joinCol) \
  .select('doc')"

df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", query) \
    .load()
```

Calling `df.show()` will then show the URI and JSON contents of the document associated with each row. The Python 
[from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html)
function can then be used to parse the contents of each `doc` column into a JSON object as needed. 

### Streaming support

The connector supports
[streaming reads](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) from MarkLogic
via micro-batches. The connector configuration does not change; instead, different Spark APIs are used to read a 
stream of data from MarkLogic.

The below PySpark example demonstrates how to stream micro-batches from MarkLogic to the console (streaming to the 
console is only recommend for demonstration and debugging); this can be run against the MarkLogic application 
deployed in the [Getting Started with PySpark guide](getting-started/pyspark.md):

```
stream = spark.readStream \
    .format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.numPartitions", 2) \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load() \
    .writeStream \
    .format("console") \
    .start()
stream.processAllAvailable()
stream.stop()
```

Micro-batches are constructed based on the number of partitions and user-defined batch size; more information on each
setting can be found in section below on tuning performance. Each request to MarkLogic that is made in "batch read"
mode - i.e. when using Spark's `read` function instead of `readStream` - corresponds to a micro-batch when reading
data via a stream. In the example above, which uses the connector's default batch size of 100,000 rows and 2 
partitions, 2 calls are made to MarkLogic, resulting in two micro-batches. 

The number of micro-batches can be determined by enabling info-level logging and looking for a message similar to:

    Partition count: 2; number of requests that will be made to MarkLogic: 2

### Pushing down operations

The Spark connector framework supports pushing down multiple operations to the connector data source. This can
often provide a significant performance boost by allowing the data source to perform the operation, which can result in
both fewer rows returned to Spark and less work for Spark to perform. The MarkLogic connector supports pushing
down the following operations to MarkLogic:

- `count`
- `drop` and `select`
- `filter` and `where`
- `groupBy` plus any of `avg`, `count`, `max`, `mean`, `min`, or `sum`
- `limit`
- `orderBy` and `sort`

For each of the above operations, the user's Optic query is enhanced to include the associated Optic function. Note 
that if multiple partitions are used to perform the `read` operation, each partition will apply the above 
functions on the rows that it retrieves from MarkLogic. Spark will then merge the results from each partition and 
apply the aggregation to ensure that the correct response is returned. 

In the following example, every operation after `load()` is pushed down to MarkLogic, thereby resulting in far fewer 
rows being returned to Spark and far less work having to be done by Spark:

```
spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee', '')") \
    .load() \
    .filter("HiredDate < '2020-01-01'") \
    .groupBy("State", "Department") \
    .count() \
    .orderBy("State", "count") \
    .limit(10) \
    .show()
```

The following results are returned:

```
+-----+-----------+-----+                                                       
|State| Department|count|
+-----+-----------+-----+
|   AL|  Marketing|    1|
|   AL|   Training|    1|
|   AL|        R&D|    4|
|   AL|      Sales|    4|
|   AR|      Sales|    1|
|   AR|  Marketing|    3|
|   AR|        R&D|    9|
|   AZ|   Training|    1|
|   AZ|Engineering|    2|
|   AZ|        R&D|    2|
+-----+-----------+-----+
```

### Disabling push down of aggregates

If you run into any issues with aggregates being pushed down to MarkLogic, you can set the 
`spark.marklogic.read.pushDownAggregates` option to `false`. If doing so results in what appears to be a different and 
correct result, please [file an issue with this project](https://github.com/marklogic/marklogic-spark-connector/issues).

### Tuning performance

The primary factor affecting connector performance when reading rows is how many requests are made to MarkLogic. In
general, performance will be best when minimizing the number of requests to MarkLogic while ensuring that no single
request attempts to return or process too much data.

Two [configuration options](configuration.md) control how many requests are made. First, the
`spark.marklogic.read.numPartitions` option controls how many partitions are created. For each partition, Spark
will use a separate task to send requests to MarkLogic to retrieve rows matching your Optic DSL query. Second, the
`spark.marklogic.read.batchSize` option controls approximately how many rows will be retrieved in each call to
MarkLogic.

To understand how these options control the number of requests to MarkLogic,
consider an Optic query that matches 10 million rows in MarkLogic, a partition count of 10, and a batch size of
100,000 rows (the default value). This configuration will result in the connector creating 10 Spark partition readers,
each of which will retrieve approximately 1,000,000 unique rows. And with a batch size of 100,000, each partition
reader will make approximately 10 calls to MarkLogic to retrieve these rows, for a total of 100 calls across all
partitions.

Performance should be tested by varying the number of partitions and the batch size. In general, increasing the
number of partitions should help performance as the number of rows to return increases. Determining the optimal batch
size depends both on the number of columns in each returned row and what kind of Spark operations are being invoked.
The next section describes both how the connector tries to optimize performance when an aggregation is performed
and when the same kind of optimization should be made when not many rows need to be returned.

#### Optimizing for smaller result sets

If your Optic query matches a set of rows whose count is a small percentage of the total number of rows in
the view that the query runs against, you should find improved performance by setting `spark.marklogic.read.batchSize`
to zero. This setting ensures that for each partition, a single request is sent to MarkLogic.

If your Spark program includes an aggregation that the connector can push down to MarkLogic, then the connector will
automatically use a batch size of zero unless you specify a different value for `spark.marklogic.read.batchSize`. This
optimization should typically be desirable when calculating an aggregation, as MarkLogic will return far fewer rows
per request depending on the type of aggregation.

If the result set matching your query is particularly small - such as tens of thousands of rows or less, or possibly
hundreds of thousands of rows or less - you may find optimal performance by setting
`spark.marklogic.read.numPartitions` to one. This will result in the connector sending a single request to MarkLogic.
The effectiveness of this approach can be evaluated by executing the Optic query via
[MarkLogic's qconsole application](https://docs.marklogic.com/guide/qconsole/intro), which will execute the query in
a single request as well.

#### More detail on partitions

This section is solely informational and is not required understanding for using the connector
successfully.

For MarkLogic users familiar with the [Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement), the
concept of a "partition" may suggest that the number and location of forests in a database play a role. But due to
the ability of an Optic query to perform joins across documents, the goal of running a query against a single forest
is not meaningful, as constructing a row may require data from documents in many forests across many hosts.

Instead of referring to forests, a "partition" in the scope of the connector refers to a set of
internal row identifiers that are generated at a particular server timestamp. Each row matching an Optic query is
assigned a random identifier, which can be any number in the range from zero to the max unsigned long value. A
partition is a slice of numbers within that range. Because row identifiers are generated randomly, matching rows
can be expected to be fairly evenly distributed across each partition, but the number of rows in each partition will
most likely not be equal.

Because row identifiers are specific to a MarkLogic server timestamp, all requests sent to MarkLogic for a given
query will use the same server timestamp, thus ensuring consistent results. This behavior is analogous to the use of a
[consistent snapshot](https://docs.marklogic.com/guide/java/data-movement#id_18227) when using the Data Movement SDK.


## Reading rows via custom code

Rows can be retrieved via custom code executed by MarkLogic and written in either JavaScript or XQuery. This can be
useful when you need to retrieve data that cannot be easily accessed via Optic, or you are looking for behavior similar 
to that of [MarkLogic's CoRB tool](https://github.com/marklogic-community/corb2) for processing data already in 
MarkLogic. 

Custom code can be [written in JavaScript](https://docs.marklogic.com/guide/getting-started/javascript) by 
configuring the `spark.marklogic.read.javascript` option:

```
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, null, cts.collectionQuery('employee'))") \
    .load()
```

Or code can be [written in XQuery](https://docs.marklogic.com/guide/getting-started/XQueryTutorial) by configuring the 
`spark.marklogic.read.xquery` option:

```
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.xquery", "cts:uris((), (), cts:collection-query('employee'))") \
    .load()
```

You can also invoke a JavaScript or XQuery module in your application's modules database via the 
`spark.marklogic.read.invoke` option:

```
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.invoke", "/read.sjs") \
    .load()
```

### Custom code schemas

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
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.invoke", "/read-custom-schema.sjs") \
    .schema(StructType([StructField("id", IntegerType()), StructField("name", StringType())])) \
    .load()
```

### Custom external variables

You can pass external variables to your custom code by configuring one or more options with names starting with 
`spark.marklogic.read.vars.`. The remainder of the option name will be used as the external variable name, and the value
of the option will be sent as the external variable value. Each external variable will be passed as a string due to 
Spark capturing all option values as strings.

The following demonstrates two custom external variables being configured and used by custom JavaScript code:

```
df = spark.read.format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.vars.var1", "Engineering") \
    .option("spark.marklogic.read.vars.var2", "Marketing") \
    .option("spark.marklogic.read.javascript", "var var1, var2; cts.uris(null, null, cts.wordQuery([var1, var2]))") \
    .load()
```

### Streaming support

Spark's support for [streaming reads](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) 
from MarkLogic can be useful when your custom code for reading data may take a long time to execute. Or, based on the
nature of your custom code, running the query incrementally to produce smaller batches may be a better fit for your 
use case. 

(TODO This needs to be rewritten, will do so in a follow up PR.)

To stream results from your custom code, the connector must know how batches can be constructed based on the results of
your custom code. Because the connector does not know anything about your code, the connector needs to run an 
additional set of custom code that you implement to provide a sequence of partitions to the connector. The
connector will then run your custom once for each of your partitions, with the partition being passed as
an external variable to your custom code. 

The code to run for providing a sequence of partitions must be defined via one of the following options:

- `spark.marklogic.read.partitions.invoke` - a JavaScript or XQuery module path to invoke.
- `spark.marklogic.read.partitions.javascript` - a JavaScript program to evaluate.
- `spark.marklogic.read.partitions.xquery` - an XQuery program to evaluate.

Note that any variables you define via the `spark.marklogic.reads.vars` prefix will also be sent to the above code, 
in addition to the code you define for reading rows. 

You are free to return any sequence of partitions. For each one, the connector will invoke your regular custom
code with an external variable named `PARTITION` of type `String`. You are then free to use this value to return 
a set of results associated with the partition.

The following examples illustrates how the forest IDs for the `spark-example-content` database can be used as batch
identifiers. The custom code for returning URIs is then constrained to the value of `PARTITION` which will be a forest 
ID. Spark will invoke the custom code once for each partition, with the returned batch of rows being immediately 
sent to the writer, which in this example are then printed to the console:

```
stream = spark.readStream \
    .format("com.marklogic.spark") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.partitions.javascript", "xdmp.databaseForests(xdmp.database('spark-example-content'))") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, null, cts.collectionQuery('employee'), null, [PARTITION]);") \
    .load() \
    .writeStream \
    .format("console") \
    .start()
stream.processAllAvailable()
stream.stop()
```

For a streaming use case, you may wish to ensure that every query runs 
[at the same point in time](https://docs.marklogic.com/guide/app-dev/point_in_time). Because you are free to return
any partitions you wish, one technique for accomplishing this would be to construct partitions
containing both a forest ID and a server timestamp:

```
const forestIds = xdmp.databaseForests(xdmp.database('spark-example-content'))
const timestamp = xdmp.requestTimestamp()
Sequence.from(forestIds.toArray().map(forestId => forestId + ":" + timestamp))
```

In your custom code, you would then parse out the forest ID and server timestamp from each partition and use
them accordingly in your queries. The MarkLogic documentation in the link above can provide more details and examples
on how to perform point-in-time queries with server timestamps.

### Tuning performance

A key difference with reading via custom code is that unless you are using Spark streaming, a single call will be made 
to MarkLogic to execute the custom code. Therefore, the options `spark.marklogic.read.batchSize` and 
`spark.marklogic.read.numPartitions` will not have any impact on the performance of executing custom code. 
You must therefore ensure that your custom code can be expected to complete in a single call to MarkLogic without 
timing out. The support for Spark streaming described above can help if your query is at risk of timing out, as you can
effectively break it up into many smaller queries. However, note that Spark streaming sends many datasets to the writer
instead of a single dataset; this may not be appropriate for your use case.

## Error handling

When reading data with the connector, any error that occurs - whether it is before any data is read or during a request
to MarkLogic to read data - is immediately thrown to the user and the read operation ends without any result being
returned. This is consistent with the design of Spark connectors, where a partition reader is allowed to throw
exceptions and is not given any mechanism for reporting an error and continuing to read data. The connector will strive
to provide meaningful context when an error occurs to assist with debugging the cause of the error.

In practice, it is expected that most errors will be a result of a misconfiguration. For example, the connection and
authentication options may be incorrect, or the Optic query may have a syntax error. Any errors that cannot be
fixed via changes to the options passed to the connector should be
[reported as new issues](https://github.com/marklogic/marklogic-spark-connector/issues) in the connector's GitHub
repository.


