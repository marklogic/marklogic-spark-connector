---
layout: default
title: Reading document rows
parent: Reading Data
nav_order: 3
---

The connector supports reading documents from MarkLogic via a variety of MarkLogic search queries, with each 
document being returned in a row that captures the document URI, content, and metadata. This approach is useful for
when documents either need to copied as-is to another location (including a different MarkLogic database), or 
when data needs to be retrieved and an [Optic query](optic.md) is not a practical option.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Usage

This will be cleaned up before the 2.2.0 release, just getting the basics in place. 

General approach is to specify any combination of a string query, a complex query, collections, and a directory. A 
string query is configured via `spark.marklogic.read.documents.stringQuery`:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.stringQuery", "Engineering OR Marketing") \
    .load()
df.show()
```

You can also submit a [structured query](https://docs.marklogic.com/guide/search-dev/structured-query#), a 
[serialized CTS query](https://docs.marklogic.com/guide/rest-dev/search#id_30577), or a 
[combined query](https://docs.marklogic.com/guide/rest-dev/search#id_69918) via 
`spark.marklogic.read.documents.query`, which can be combined with a string query as well:

```
# Structured query
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.query", '{"query": {"queries": [{"term-query": {"text": ["Engineering"]} }] } }') \
    .load()
df.show()

# Serialized CTS query
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.query", '{"ctsquery": {"wordQuery": {"text": "Engineering"}}}') \
    .load()
df.show()

# Combined query
query = "<search xmlns='http://marklogic.com/appservices/search'>\
        <options><constraint name='c1'><word><element name='Department'/></word></constraint></options>\
        <qtext>c1:Engineering</qtext></search>"

df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.query", query) \
    .load()
df.show()
```

## Querying by collections

You can read document rows by specifying comma-delimited collections via `spark.marklogic.read.documents.collections`:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .load()
df.show()
```

You can also specify collections with any of the above queries:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .option("spark.marklogic.read.documents.stringQuery", "Marketing") \
    .load()
df.show()
```

## Querying by directory

Similar to querying by collections, you can query only by a directory or include it with another query:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.directory", "/employee/") \
    .load()
df.show()
```

## Using query options

If you have a set of [MarkLogic query options](https://docs.marklogic.com/guide/search-dev/query-options) installed in 
your REST API app server, you can reference these via `spark.marklogic.read.documents.options`.

## Requesting document metadata

By default, each document row will only have its `URI`, `content`, and `format` columns populated. You can use the 
`spark.marklogic.read.documents.categories` option to request metadata for each document. The value of the option 
must be a comma-delimited list of one or more of the following values: 

- `content` will result in the `content` and `format` columns being populated. If excluded, neither will be populated.
- `metadata` will result in all metadata columns - collections, permissions, quality, properties, and metadata values - 
being populated.
- `collections`, `permissions`, `quality`, `properties`, and `metadatavalues` can be used to request each metadata type
without retrieving all of them.

A value of `content,metadata` will return the document content and all metadata for each document:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .option("spark.marklogic.read.documents.categories", "content,metadata") \
    .load()
df.show(2)

# Displays:
+--------------------+--------------------+------+-----------+--------------------+-------+----------+--------------+
|                 URI|             content|format|collections|         permissions|quality|properties|metadataValues|
+--------------------+--------------------+------+-----------+--------------------+-------+----------+--------------+
|/employee/70325be...|[7B 22 47 55 49 4...|  JSON| [employee]|{rest-reader -> [...|      0|        {}|            {}|
|/employee/58ef1ba...|[7B 22 47 55 49 4...|  JSON| [employee]|{rest-reader -> [...|      0|        {}|            {}|
+--------------------+--------------------+------+-----------+--------------------+-------+----------+--------------+
```

A value of `collections,permissions`

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .option("spark.marklogic.read.documents.categories", "collections,permissions") \
    .load()
df.show(2)

# Displays:
+--------------------+-------+------+-----------+--------------------+-------+----------+--------------+
|                 URI|content|format|collections|         permissions|quality|properties|metadataValues|
+--------------------+-------+------+-----------+--------------------+-------+----------+--------------+
|/employee/015e8ac...|   null|  null| [employee]|{rest-reader -> [...|   null|      null|          null|
|/employee/027c016...|   null|  null| [employee]|{rest-reader -> [...|   null|      null|          null|
+--------------------+-------+------+-----------+--------------------+-------+----------+--------------+
```

## Using a REST transform

If you have a [MarkLogic REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) installed in your REST 
API app server, you can reference it via the following options:

- `spark.marklogic.read.documents.transform` = specifies the name of the REST transform.
- `spark.marklogic.read.documents.transformParams` = optional; comma-delimited list of parameter names and values; e.g.
param1,value1,param2,value2.
- `spark.marklogic.read.documents.transformParamsDelimiter` = optiona, defaults to a comma; override this in case you 
have a transform parameter with a comma in the value.

## Manipulating the content column

The content of each document is in a column named `content` with a type of `binary`. You may wish to manipulate this
value, such as when the document is JSON. The following example shows one technique for casting the column values to
strings and then reading the first row's content as JSON:

```
import json
from pyspark.sql.functions import col

df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.stringQuery", "Engineering") \
    .load()

df2 = df.select(col("content").cast("string"))
doc = json.loads(df2.head()['content'])
doc['Department']
```

## Understanding performance

The connector mimics the behavior of the [MarkLogic Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement)
by creating Spark partition readers that are assigned to a specific forest. By default, the connector will create 
4 readers per forest. You can use the `spark.marklogic.read.documents.partitionsPerForest` option to control
the number of readers. You should adjust this based on your cluster configuration. For example,a default REST API app 
server will have 32 server threads and 3 forests per host. 4 partition readers will thus consume 12 of the 32 server
threads. If the app server is not servicing any other requests, performance will typically be improved by configuring
8 partitions per forest. Note that the `spark.marklogic.read.numPartitions` option does not have any impact;
that is only used when reading via an Optic query.

Each partition reader will make one to many calls to MarkLogic to retrieve documents. The 
`spark.marklogic.read.batchSize` option controls how many documents will be retrieved in a call. The value defaults
to 500. For smaller documents, particularly those with 10 elements or fewer, you may find a batch size of 1,000 or 
even 10,000 to provide better performance.

As an example, consider a query that matches 120,000 documents in a cluster with 3 hosts and 2 forests on each host. 
The connector will default to creating 24 partitions - 4 for each of the 6 forests. Each partition reader will read
approximately 5,000 documents. With a default batch size of 500, each partition reader will make approximately 10 
calls to MarkLogic (these numbers are all approximate as a forest may have slightly more or less than 20,000 documents).
Depending on the size of the documents and whether the cluster is servicing other requests, performance may improve
with more partition readers and a higher batch size. 

You can also adjust the level of parallelism by controlling how many threads Spark uses for executing partition reads. 
Please see your Spark distribution's documentation for further information.

