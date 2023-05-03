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
[op.fromView](https://docs.marklogic.com/op.fromView) accessor function. Aside from this restriction, the query can 
perform any operations and joins that are supported by MarkLogic.

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
`spark.marklogic.client.numPartitions` option controls how many partitions are created. For each partition, Spark 
will use a separate task to make requests to MarkLogic to retrieve rows matching your Optic DSL query. Second, the 
`spark.marklogic.client.batchSize` option controls approximately how many rows will be retrieved in each call to 
MarkLogic. 

These two options impact each other in terms of how many tasks are used to make requests to MarkLogic. For example, 
consider an Optic DSL query that matches 1 million rows in MarkLogic, a partition count of 10, and a batch size of 
10,000 rows (the default value). This configuration will result in the connector creating 10 Spark partition readers,
each of which will retrieve approximately 100,000 unique rows. And with a batch size of 10,000, each partition 
reader will make approximately 10 calls to MarkLogic to retrieve these rows, for a total of 100 calls across all 
partitions. Performance can be tested by varying the number of partitions and the batch size. The ideal settings 
will depend highly on your Spark and MarkLogic environments. 

TODO Should we explain exactly how the query is partitioned in MarkLogic? That is really implementation detail - the 
user doesn't need to know about row IDs - but it may be useful to describe. 
