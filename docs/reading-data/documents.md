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

To read documents from MarkLogic, you must specify at least one of 4 supported query types described below - a string 
query; a structured, serialized CTS, or combined query; a collection query; or a directory query. You may specify any
combination of those 4 query types as well. 

You can specify a [string query](https://docs.marklogic.com/guide/search-dev/string-query) that utilizes 
MarkLogic's search grammar via the `spark.marklogic.read.documents.stringQuery` option:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.stringQuery", "Engineering OR Marketing") \
    .load()
df.show()
```

The document content is in a column named `content` of type `binary`. See further below for an example of how to use
common Spark functions to cast this value to a string or parse it into a JSON object. 

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
df.count()

# Serialized CTS query
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.query", '{"ctsquery": {"wordQuery": {"text": "Engineering"}}}') \
    .load()
df.show()
df.count()

# Combined query
query = "<search xmlns='http://marklogic.com/appservices/search'>\
        <options><constraint name='c1'><word><element name='Department'/></word></constraint></options>\
        <qtext>c1:Engineering</qtext></search>"

df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.query", query) \
    .load()
df.show()
df.count()
```

## Querying by collections

You can read document rows by specifying comma-delimited collections via `spark.marklogic.read.documents.collections`:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .load()
df.show()
df.count()
```

You can also specify collections with any of the above queries:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .option("spark.marklogic.read.documents.stringQuery", "Marketing") \
    .load()
df.show()
df.count()
```

## Querying by directory

Similar to querying by collections, you can query only by a directory or include it with another query:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.directory", "/employee/") \
    .load()
df.show()
df.count()
```

## Using query options

If you have a set of [MarkLogic query options](https://docs.marklogic.com/guide/search-dev/query-options) installed in 
your REST API app server, you can reference these via `spark.marklogic.read.documents.options`. You will then typically
use the `spark.marklogic.read.documents.stringQuery` option and reference one or more constraints defined in your 
query options.

## Requesting document metadata

By default, each row will only have its `URI`, `content`, and `format` columns populated. You can use the 
`spark.marklogic.read.documents.categories` option to request metadata for each document. The value of the option 
must be a comma-delimited list of one or more of the following values: 

- `content` will result in the `content` and `format` columns being populated. If excluded from the option value, neither will be populated.
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
|/employee/70325be...|[7B 22 47 55 49 4...|  JSON| [employee]|{rest-reader -> [...|      0|      null|            {}|
|/employee/58ef1ba...|[7B 22 47 55 49 4...|  JSON| [employee]|{rest-reader -> [...|      0|      null|            {}|
+--------------------+--------------------+------+-----------+--------------------+-------+----------+--------------+
```

Note that the Spark `show()` function allows for results to be displayed in a vertical format instead of in a table. 
You can more easily see values in the metadata columns by requesting a vertical format and dropping the `content` column:

```
df.drop("content").show(2, 0, True)
```

A value of `collections,permissions` will result in the `content` and `format` columns being empty and the `collections`
and `permissions` columns being populated:

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

## Filtered searches

The connector defaults to [unfiltered searches in MarkLogic](https://docs.marklogic.com/guide/performance/unfiltered). 
Assuming you have sufficient indexes configured for your query, an unfiltered search will return correct results with
optimal performance.

However, as noted in the above linked documentation, a query may need to be "filtered" to ensure that the returned 
results are accurate. If your query and index configuration meet this need, you can use the following option to 
request a filtered search:

    .option("spark.marklogic.read.documents.filtered", "true")

Filtered searches are generally slower, and you should be careful with this setting for larger result sets. However, 
the cost of a filtered search may be outweighed by the connector having to return far fewer results. In that scenario,
a filtered search will both return accurate results and may be faster. Ideally though, you can configure indexes on your
database to allow for an unfiltered search, which will return accurate results and be faster than a filtered search.

## Using secondary URIs queries

As of version 2.7.0, the connector supports executing secondary queries to retrieve additional URIs beyond those specified in your initial document query. This feature is useful when you need to read documents that are related to your initial set of documents through shared data values or other relationships.

When using secondary URIs queries, the connector will first retrieve the URIs from your primary query (via `spark.marklogic.read.documents.uris` or other document query options), then execute your secondary query code with access to those URIs, and finally return documents for both the original URIs and any additional URIs returned by the secondary query.

### Basic usage

You can execute a secondary query using JavaScript via the `spark.marklogic.read.secondaryUris.javascript` option:

```python
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.uris", "/author/author1.json\n/author/author2.json") \
    .option("spark.marklogic.read.secondaryUris.javascript", """
        var URIs;
        const citationIds = cts.elementValues(xs.QName("CitationID"), null, null, cts.documentQuery(URIs));
        cts.uris(null, null, cts.andQuery([
            cts.notQuery(cts.documentQuery(URIs)),
            cts.collectionQuery('author'),
            cts.jsonPropertyValueQuery('CitationID', citationIds)
        ]))
    """) \
    .load()
df.show()
```

Or using XQuery via the `spark.marklogic.read.secondaryUris.xquery` option:

```python
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.uris", "/author/author1.json\n/author/author2.json") \
    .option("spark.marklogic.read.secondaryUris.xquery", """
        declare namespace json = "http://marklogic.com/xdmp/json";
        declare variable $URIs external;
        let $values := json:array-values($URIs)
        let $citationIds := cts:element-values(xs:QName("CitationID"), (), (), cts:document-query($values))
        return cts:uris((), (), cts:and-query((
            cts:not-query(cts:document-query($values)),
            cts:collection-query('author'),
            cts:json-property-value-query('CitationID', $citationIds)
        )))
    """) \
    .load()
df.show()
```

### Available URIs in secondary queries

Your secondary query code has access to the URIs from your primary query through:

- **JavaScript**: An external variable named `URIs` containing the array of URIs
- **XQuery**: An external variable named `$URIs` containing a JSON array of the URIs

The examples above show how to use these URIs to find related documents - in this case, finding other author documents that share the same CitationID values as the original documents.

### Using module invocation

You can invoke a JavaScript or XQuery module from your application's modules database via the `spark.marklogic.read.secondaryUris.invoke` option:

```python
option("spark.marklogic.read.secondaryUris.invoke", "/findRelatedAuthors.xqy")
```

### Using local files

You can specify local file paths containing either JavaScript or XQuery code via the `spark.marklogic.read.secondaryUris.javascriptFile` and `spark.marklogic.read.secondaryUris.xqueryFile` options:

```python
.option("spark.marklogic.read.secondaryUris.javascriptFile", "/path/to/findRelatedAuthors.js") \
```

### Custom external variables

You can pass external variables to your secondary query code by configuring options with names starting with `spark.marklogic.read.secondaryUris.vars.`. The remainder of the option name will be used as the external variable name:

```python
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.uris", "/author/author1.json") \
    .option("spark.marklogic.read.secondaryUris.javascript", "Sequence.from([URI1, URI2])") \
    .option("spark.marklogic.read.secondaryUris.vars.URI1", "/author/author2.json") \
    .option("spark.marklogic.read.secondaryUris.vars.URI2", "/author/author3.json") \
    .load()
df.show()
```

### Use cases

Secondary URIs queries are particularly useful for:

- **Document relationships**: Finding documents that reference or are referenced by your initial set
- **Hierarchical data**: Retrieving parent or child documents in a hierarchy
- **Cross-references**: Finding documents that share common property values
- **Graph traversal**: Following relationships between documents to expand your result set
- **Data enrichment**: Adding related documents to provide fuller context for analysis

The secondary query is executed after the primary document selection, allowing you to build complex multi-step queries that would be difficult to express in a single MarkLogic search operation.

## Tuning performance

The connector mimics the behavior of the [MarkLogic Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement)
by creating Spark partition readers that are assigned to a specific forest. By default, the connector will create 
4 readers per forest. Each reader will read URIs and documents in a specific range of URIs at a specific MarkLogic 
server timestamp, ensuring both that every matching document is retrieved and that the same document is never returned 
more than once for a query.

You can use the `spark.marklogic.read.documents.partitionsPerForest` option to control the number of readers. You 
should adjust this based on your cluster configuration. For example, a default REST API app server will have 32 server 
threads and 3 forests per host. 4 partition readers will thus utilize 12 of the 32 server threads. If the app server 
is not servicing any other requests, performance will typically be improved by configuring 8 partitions per forest. 
Note that the `spark.marklogic.read.numPartitions` option does not have any impact; that is only used when reading 
via an Optic query.

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

### Using a load balancer

If your MarkLogic cluster has multiple hosts, it is highly recommended to put a load balancer in front
of your cluster and have the connector connect through the load balancer. A typical load balancer will help ensure 
not only that load is spread across the hosts in your cluster, but that any network or connection failures can be 
retried without the error propagating to the connector. 

### Direct connections to hosts

If you do not have a load balancer in front of your MarkLogic cluster, and your Spark program is able to connect to 
each host in your MarkLogic cluster, you can set the
`spark.marklogic.client.connectionType` option to `direct`. Each partition reader will then connect to the
host on which the reader's assigned forest resides. This will typically improve performance by reducing the network
traffic, as the host that receives a request will not need to involve any other host in the processing of that request.
