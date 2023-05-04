---
layout: default
title: Reading Data
nav_order: 4
---

The MarkLogic Spark connector allows for data to be retrieved from MarkLogic as rows via an 
[Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710). The 
sections below provide more detail about how this process works and how it can be controlled. 

## Query requirements

As of the 1.0 release of the connector, the Optic DSL query must use the 
[op.fromView](https://docs.marklogic.com/op.fromView) accessor function. The query must also adhere to the 
restrictions that the 
[RowBatcher in the Data Movement SDK](https://github.com/marklogic/java-client-api/wiki/Row-Batcher#building-a-plan-for-exporting-the-view)
adheres to as well. 

## Schema inference

The MarkLogic Spark connector will infer a Spark schema automatically based on the view that is used referenced in 
the Optic DSL query. Each column returned by your Optic DSL query will be mapped to a Spark schema column with the 
same name and an appropriate type. 

You may override this feature and provide your own schema instead. The example below shows how a custom schema can 
be provided within PySpark; this assumes that you have deployed the application in the 
[Getting Started with PySpark](getting-started-pyspark.md) guide:

```
from pyspark.sql.types import StructField, StructType, StringType
df = spark.read.format("com.marklogic.spark")\
    .schema(StructType([StructField("example.employee.GivenName", StringType()), StructField("example.employee.Surname", StringType())]))\
    .option("spark.marklogic.client.host", "localhost")\
    .option("spark.marklogic.client.port", "8020")\
    .option("spark.marklogic.client.username", "pyspark-example-user")\
    .option("spark.marklogic.client.password", "password")\
    .option("spark.marklogic.client.authType", "digest")\
    .option("spark.marklogic.read.opticDsl", "op.fromView('example', 'employee')")\
    .load()
```

## Tuning performance

The primary factor affecting how quickly the MarkLogic Spark connector can retrieve rows is MarkLogic's ability to 
process your Optic DSL query. The 
[MarkLogic Optic performance documentation](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_91398) can help with 
optimizing your query to maximize performance. 

Two [configuration options](configuration.md) in the connector will also impact performance. First, the 
`spark.marklogic.read.numPartitions` option controls how many partitions are created. For each partition, Spark 
will use a separate task to send requests to MarkLogic to retrieve rows matching your Optic DSL query. Second, the 
`spark.marklogic.read.batchSize` option controls approximately how many rows will be retrieved in each call to 
MarkLogic. 

These two options impact each other in terms of how many tasks are used to make requests to MarkLogic. For example, 
consider an Optic DSL query that matches 1 million rows in MarkLogic, a partition count of 10, and a batch size of 
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

If your Optic DSL query matches a set of rows whose count is a small percentage of the total number of rows in 
the view that the query runs against, you may find improved performance by setting `spark.marklogic.read.batchSize` 
to zero. Doing so ensures that for each partition, a single request is sent to MarkLogic. 

If the result set matching your query is particularly small - such as thousands of rows or less, or possibly tens of 
thousands of rows or less - you may find optimal performance by also setting `spark.marklogic.read.numPartitions` to 
one. This will result in the connector sending a single request to MarkLogic. 

### More detail on partitions

This section is solely informational and is not required understanding for using the MarkLogic Spark connector 
successfully. 

For MarkLogic users familiar with the [Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement), the 
concept of a "partition" may suggest that the number and location of forests in a database play a role. But due to 
the ability of an Optic query to perform joins across documents, the goal of running a query against a single forest 
is not meaningful, as constructing a row may require data from documents in many forests across many hosts. 

Instead of referring to forests, a "partition" in the scope of the MarkLogic Spark connector refers to a set of 
internal row identifiers that are generated at a particular server timestamp. Each row matching an Optic query is 
assigned a random identifier, which can be any number in the range from zero to the max unsigned long value. A 
partition is then a slice of numbers within that range. Because row identifiers are generated randomly, matching rows 
can be expected to be fairly evenly distributed across each partition, but the number of rows in each partition will 
most likely not be equal.  

Because row identifiers are specific to a MarkLogic server timestamp, all requests sent to MarkLogic for a given 
query will use the same server timestamp, thus ensuring consistent results. This behavior is analogous to the use of a 
[consistent snapshot](https://docs.marklogic.com/guide/java/data-movement#id_18227) when using the Data Movement SDK.
