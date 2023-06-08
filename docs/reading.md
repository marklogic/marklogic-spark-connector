---
layout: default
title: Reading Data
nav_order: 3
---

The MarkLogic Spark connector allows for data to be retrieved from MarkLogic as rows via an 
[Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710). The 
sections below provide more detail on configuring how data is retrieved and converted into a Spark DataFrame.

## Query requirements

As of the 2.0 release of the connector, the Optic query must use the 
[op.fromView](https://docs.marklogic.com/op.fromView) accessor function. The query must also adhere to the 
restrictions that the 
[RowBatcher in the Data Movement SDK](https://github.com/marklogic/java-client-api/wiki/Row-Batcher#building-a-plan-for-exporting-the-view)
adheres to as well. 

## Schema inference

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
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8020") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load()
```

## Accessing documents 

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
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8020") \
    .option("spark.marklogic.read.opticQuery", query) \
    .load()
```

Calling `df.show()` will then show the URI and JSON contents of the document associated with each row. The Python 
[from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html)
function can then be used to parse the contents of each `doc` column into a JSON object as needed. 

## Streaming support

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
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8020") \
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
data via a stream. In the example above, which uses the connector's default batch size of 10,000 rows and 2 
partitions, 2 calls are made to MarkLogic, resulting in two micro-batches. 

The number of micro-batches can be determined by enabling info-level logging and looking for a message similar to:

    Partition count: 2; number of requests that will be made to MarkLogic: 2

## Error handling

When reading data with the connector, any error that occurs - whether it is before any data is read or during a request
to MarkLogic to read data - is immediately thrown to the user and the read operation ends without any result being 
returned. This is consistent with the design of Spark connectors, where a partition reader is allowed to throw 
exceptions and is not given any mechanism for reporting an error and continuing to read data. The connector will strive 
to provide meaningful context when an error occurs to assist with debugging the cause of the error. 

In practice, it is expected that most errors will be a result of a misconfiguration. For example, the connection and 
authentication options may be incorrect, or the Optic query may have a syntax error. Any errors that cannot be 
fixed via changes to the options passed to the connector should be reported as new issues to this GitHub repository.

## Pushing down operations

The Spark connector framework supports pushing down multiple operations to the connector data source. This can
often provide a significant performance boost by allowing the data source to perform the operation, which can result in
both fewer rows returned to Spark and less work for Spark to perform. The connector supports pushing
down the following operations to MarkLogic:

- `count`
- `drop` and `select`
- `filter` and `where`
- `groupBy` when followed by `count`
- `limit`
- `orderBy`

For each of the above operations, user's Optic query is enhanced to include the associated Optic function.
Note that if multiple partitions are used to perform the `read` operation, each
partition will apply the above functions on the rows that it retrieves from MarkLogic. Spark will then merge the results
from each partition and re-apply the function calls as necessary to ensure that the correct response is returned.

## Tuning performance

The primary factor affecting how quickly the connector can retrieve rows is MarkLogic's ability to process your Optic 
query. The 
[MarkLogic Optic performance documentation](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_91398) can help with 
optimizing your query to maximize performance. 

Two [configuration options](configuration.md) in the connector will also impact performance. First, the 
`spark.marklogic.read.numPartitions` option controls how many partitions are created. For each partition, Spark 
will use a separate task to send requests to MarkLogic to retrieve rows matching your Optic DSL query. Second, the 
`spark.marklogic.read.batchSize` option controls approximately how many rows will be retrieved in each call to 
MarkLogic. 

These two options impact each other in terms of how many tasks are used to make requests to MarkLogic. For example, 
consider an Optic query that matches 1 million rows in MarkLogic, a partition count of 10, and a batch size of 
10,000 rows (the default value). This configuration will result in the connector creating 10 Spark partition readers,
each of which will retrieve approximately 100,000 unique rows. And with a batch size of 10,000, each partition 
reader will make approximately 10 calls to MarkLogic to retrieve these rows, for a total of 100 calls across all 
partitions. 

Performance can thus be tested by varying the number of partitions and the batch size. In general, increasing the 
number of partitions should help performance as the number of matching rows increases. A single partition may suffice 
for a query that returns thousands of rows or fewer, while a query that returns hundreds of millions of rows will 
benefit from dozens of partitions or more. The ideal settings will depend on your Spark and MarkLogic environments 
along with the complexity of your Optic query. Testing should be performed with different queries, partition counts, 
and batch sizes to determine the optimal settings.

### Optimizing for smaller result sets

If your Optic query matches a set of rows whose count is a small percentage of the total number of rows in 
the view that the query runs against, you may find improved performance by setting `spark.marklogic.read.batchSize` 
to zero. Doing so ensures that for each partition, a single request is sent to MarkLogic. 

If the result set matching your query is particularly small - such as thousands of rows or less, or possibly tens of 
thousands of rows or less - you may find optimal performance by also setting `spark.marklogic.read.numPartitions` to 
one. This will result in the connector sending a single request to MarkLogic. 

### More detail on partitions

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
