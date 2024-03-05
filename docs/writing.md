---
layout: default
title: Writing Data
nav_order: 4
---

The MarkLogic connector allows for rows in a Spark dataset to either be written to MarkLogic as documents or processed 
via custom code written in JavaScript or XQuery and executed in MarkLogic.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Writing rows as documents

By default, the connector will serialize each Spark row it receives into a JSON document and write it to MarkLogic.
With this approach, the incoming rows can adhere to any schema, and they will still be serialized to JSON and written
to MarkLogic, leveraging MarkLogic's schema-agnostic behavior.

As shown in the [Getting Started with PySpark guide](getting-started/pyspark.md), a minimal write operation will define
how the connector should connect to MarkLogic, the Spark mode to use, and zero or more other options:

```
df.write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.collections", "write-test") \
    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
    .option("spark.marklogic.write.uriPrefix", "/write/") \
    .mode("append") \
    .save()
```

In the above example, only `format`, `spark.marklogic.client.uri` (or the other `spark.marklogic.client` options 
that can be used to define the connection details), and `mode` (which must equal "append") are required; 
the collections, permissions , and URI prefix are optional, though it is uncommon to write documents without any 
permissions. 

### Writing file rows as documents

To support the common use case of reading files and ingesting their contents as-is into MarkLogic, the connector has
special support for rows with a schema matching that of 
[Spark's binaryFile data source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html). If the incoming
rows adhere to the `binaryFile` schema, the connector will not serialize the row into JSON. Instead, the connector will 
use the `path` column value as an initial URI for the document and the `content` column value as the document contents.
The URI can then be further adjusted as described in the "Controlling document URIs".

This feature allows for ingesting files of any type. The MarkLogic REST API will
[determine the document type](https://docs.marklogic.com/guide/rest-dev/intro#id_53367) based on the URI extension, if
MarkLogic recognizes it. If MarkLogic does not recognize the extension, and you wish to force a document type on each of
the documents, you can set the `spark.marklogic.write.fileRows.documentType` option to one of `XML`, `JSON`, or `TEXT`.

### Writing document rows

As of the 2.2.0 release, you can [read documents from MarkLogic](reading-data/documents.md). A common use case is to then write these rows
to another database, or another MarkLogic cluster, or even the same database the documents were read from, potentially
transforming them and altering their URIs. 

"Document rows" adhere to the following Spark schema, which is important to understand when writing these rows as 
documents to MarkLogic:

1. `URI` is of type `string`.
2. `content` is of type `binary`.
3. `format` is of type `string`.
4. `collections` is an array of `string`s.
5. `permissions` is a map with keys of type `string` and values that are arrays of `string`s. 
6. `quality` is an `integer`.
7. `properties` is a map with keys and values of type `string`.
8. `metadataValues` is a map with keys and values of type `string`.

Writing rows corresponding to the "document row" schema is largely the same as writing rows of any arbitrary schema, 
but bear in mind these differences:

1. All the column values will be honored if populated. 
2. The `collections` and `permissions` will be replaced - not added to - if the `spark.marklogic.write.collections` and 
`spark.marklogic.write.permissions` options are specified.
3. The `spark.marklogic.write.uriTemplate` option is less useful as only the `URI` and `format` column values are available for use in the template.

### Controlling document content

Rows in a Spark DataFrame are written to MarkLogic by default as JSON documents. Each column in a row becomes a 
top-level field in the JSON document. 

To change the content of documents, a [REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) can be 
configured via the `spark.marklogic.write.transform` option. The transform will receive a JSON document as the 
document content. This can be transformed in any manner, including into XML documents. For example, the 
[transform-from-json](https://docs.marklogic.com/json:transform-from-json) MarkLogic function could be used to 
convert the JSON document into an XML document, which then can be further modified by the code in your REST transform. 

Parameters can be passed to your REST transform via the `spark.marklogic.write.transformParams` option. The value of 
this option must be a comma-delimited string of the form `param1,value1,param2,value,etc`. For example, if your 
transform accepts parameters named "color" and "size", the following options would pass values to the transform for 
those parameter names:

    .option("spark.marklogic.write.transform", "my-transform")
    .option("spark.marklogic.write.transformParams", "color,blue,size,medium")

If one of your parameter values has a comma in it, you can change the delimiter via the 
`spark.marklogic.write.transformParamsDelimiter` option. The following options show how this would be used if one of 
the parameter values contains a comma:

    .option("spark.marklogic.write.transform", "my-transform")
    .option("spark.marklogic.write.transformParams", "my-param;has,commas")
    .option("spark.marklogic.write.transformParamsDelimiter", ";")

### Controlling document URIs

By default, the connector will construct a URI for each document beginning with a UUID and ending with `.json`. A 
prefix can be specified via `spark.marklogic.write.uriPrefix`, and the default suffix of `.json` can be modified 
via `spark.marklogic.write.uriSuffix`. For example, the following options would result in URIs of the form 
"/employee/(a random UUID value)/record.json":

    .option("spark.marklogic.write.uriPrefix", "/employee/")
    .option("spark.marklogic.write.uriSuffix", "/record.json")

If you are ingesting file rows, which have an initial URI defined by the `path` column, you can also use the
`spark.marklogic.write.uriReplace` option to perform one or more replacements on the initial URI. The value of 
this option must be a comma-delimited list of regular expression and replacement string pairs, with each replacement 
string enclosed in single quotes. For example, the following approach shows a common technique for removing most of 
the file path:

    .option("spark.marklogic.write.uriReplace", ".*/some/directory,''")

URIs can also be constructed based on column values for a given row. The `spark.marklogic.write.uriTemplate` option 
allows for column names to be referenced via braces when constructing a URI. If this option is used, the 
above options for setting a prefix, suffix, and replacement expression will be ignored, as the template defines the 
entire URI. 

For example, consider a Spark DataFrame with a set of columns including `organization` and `employee_id`. 
The following template would construct URIs based on those two columns:

    .option("spark.marklogic.write.uriTemplate", "/example/{organization}/{employee_id}.json")

Both columns should have values in each row in the DataFrame. If the connector encounters a row that does not have a 
value for any column in the URI template, an error will be thrown.

If you are writing file rows that conform to 
[Spark's binaryFile schema](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html), the `path`, 
`modificationTime`, and `length` columns will be available for use with the template. The `content` column will not be
available as it is a binary array that is not expected to be useful when constructing a URI.

### Configuring document metadata

Each document written by the connector can be assigned to zero to many 
[collections in MarkLogic](https://docs.marklogic.com/guide/search-dev/collections). Collections are specified as a 
comma-delimited list via the `spark.marklogic.write.collections` option. For example, the following will assign each 
document to collections named `employee` and `data`:

    .option("spark.marklogic.write.collections", "employee,data")

Each document can also be assigned zero to many 
[permissions in MarkLogic](https://docs.marklogic.com/guide/security/permissions). Generally, you will want to 
assign at least one read permission and one update permission so that at least some users of your application can 
read and update the documents. 

Permissions are specified as a comma-delimited list via the `spark.marklogic.write.permissions` option. The list is 
a series of MarkLogic role names and capabilities in the form of `role1,capability1,role2,capability2,etc`. For example,
the following will assign each document a read permission for the role `rest-reader` and an update permission for 
the role `rest-writer`:

    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update")

If you are using [MarkLogic's support for temporal data](https://docs.marklogic.com/guide/temporal/intro), you can 
also specify a temporal collection for each document to be assigned to via the 
`spark.marklogic.write.temporalCollection`. Each document must define values for the axes associated with the 
temporal collection. 

### Streaming support

The connector supports 
[streaming writes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) to MarkLogic. 
The connector configuration does not change; instead, different Spark APIs are used to read a stream of data and 
write that stream to MarkLogic. 

A common use case for streaming involves writing data to MarkLogic from a CSV file, where the data simply needs to be 
written to MarkLogic. The following shows an example in PySpark of streaming CSV files from a directory. 
This example can be run from the `./examples/getting-started` directory in this repository 
after following the instructions in the [Getting Started with PySpark guide](getting-started/pyspark.md) for deploying 
the example application:

```
import tempfile
from pyspark.sql.types import *
spark.readStream \
    .format("csv") \
    .schema(StructType([StructField("GivenName", StringType()), StructField("Surname", StringType())])) \
    .option("header", True) \
    .load("examples/getting-started/data/csv-files") \
    .writeStream \
    .format("marklogic") \
    .option("checkpointLocation", tempfile.mkdtemp()) \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.uriPrefix", "/streaming-example/") \
    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
    .option("spark.marklogic.write.collections", "streaming-example") \
    .start() \
    .processAllAvailable()
```

The above example will stream the data in the `./data/csv-files/100-employees.csv` file through the 
connector and into MarkLogic. This will result 100 new JSON documents in the `streaming-example` collection. 

The ability to stream data into MarkLogic can make Spark an effective tool for obtaining data from a variety of data 
sources and writing it directly to MarkLogic. 

### Tuning performance

The connector uses MarkLogic's
[Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement) for writing documents to a database. The
following options can be set to adjust how the connector performs when writing data:

- `spark.marklogic.write.batchSize` = the number of documents written in one call to MarkLogic; defaults to 100.
- `spark.marklogic.write.threadCount` = the number of threads used by each partition to write documents to MarkLogic;
  defaults to 4.

These options are in addition to the number of partitions within the Spark DataFrame that is being written to
MarkLogic. For each partition in the DataFrame, a separate instance of a MarkLogic batch writer is created, each
with its own set of threads.

Optimizing performance will thus involve testing various combinations of partition counts, batch sizes, and thread
counts. The [MarkLogic Monitoring tool](https://docs.marklogic.com/guide/monitoring/intro) can help you understand
resource consumption and throughput from Spark to MarkLogic.

**You should take care** not to exceed the number of requests that your MarkLogic cluster can reasonably handle at a
given time. A general rule of thumb is not to use more threads than the number of hosts multiplied by the number of
threads per app server. A MarkLogic app server defaults to a limit of 32 threads. So for a 3-host cluster, you should
not exceed 96 total threads. This also assumes that each host is receiving requests - either via a load balancer placed
in front of the MarkLogic cluster, or by setting the `spark.marklogic.client.connectionType` option to `direct` when 
the connector can directly connect to each host in the cluster. 

The rule of thumb above can thus be expressed as:

    Number of partitions * Value of spark.marklogic.write.threadCount <= Number of hosts * number of app server threads

### Using a load balancer

If your MarkLogic cluster has multiple hosts, it is highly recommended to put a load balancer in front
of your cluster and have the connector connect through the load balancer. A typical load balancer will help ensure
not only that load is spread across the hosts in your cluster, but that any network or connection failures can be
retried without the error propagating to the connector.

### Error handling

The connector may throw an error during one of two phases of operation - before it begins to write data to MarkLogic,
and during the writing of a batch of documents to MarkLogic.

For the first kind of error, the error will be immediately returned to the user and no data will have been written.
Such errors are often due to misconfiguration of the connector options.

For the second kind of error, the connector defaults to logging the error and asking Spark to abort the entire write
operation. Any batches of documents that were written successfully prior to the error occurring will still exist in the
database. To configure the connector to only log the error and continue writing batches of documents to MarkLogic, set
the `spark.marklogic.write.abortOnFailure` option to a value of `false`.

Similar to errors with reading data, the connector will strive to provide meaningful context when an error occurs to
assist with debugging the cause of the error.


## Processing rows via custom code

Rows can be processed via custom code instead of writing them directly to MarkLogic as documents. A common use case for
this is to achieve behavior similar to that of [MarkLogic's CoRB tool](https://github.com/marklogic-community/corb2).
To easily support that use case, the connector defaults to assuming that each row has a schema containing a single 
column named "URI" of type string. This matches the convention for reading rows via custom code, which defaults to the
same schema. User-defined custom code is then expected to declare an external variable named "URI".

When using this feature, please ensure that your MarkLogic user has the required privileges for the
MarkLogic REST [eval endpoint](https://docs.marklogic.com/REST/POST/v1/eval) and
[invoke endpoint](https://docs.marklogic.com/REST/POST/v1/invoke).

The following shows an example of reading and processing rows via custom code specified by 
`spark.marklogic.write.javascript`, where each row is expected to have a single column named "URI" (the script for
reading rows only returns the first 10 URIs to make it easier to verify that the correct data is logged; you can
find the logs in the `docker/marklogic/logs/8003_ErrorLog.txt` file in your project directory, or via the "Logs" tab 
in the MarkLogic Admin web application):

```
spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ['limit=10'], cts.collectionQuery('employee'))") \
    .load() \
    .write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.javascript", "console.log('Received URI: ' + URI);") \
    .mode("append") \
    .save()
```

Custom code can be written in XQuery and specified via `spark.marklogic.write.xquery`:

```
spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ('limit=10'), cts.collectionQuery('employee'))") \
    .load() \
    .write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.xquery", "declare variable $URI external; xdmp:log('Received URI:' || $URI)") \
    .mode("append") \
    .save()
```

Custom code can also be executed via a reference to a module in your application's modules database. In the example 
below, the module - which was deployed from the `src/main/ml-modules` directory in this project - is expected to 
declare an external variable named "URI":

```
spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ['limit=10'], cts.collectionQuery('employee'))") \
    .load() \
    .write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.invoke", "/process-uri.sjs") \
    .mode("append") \
    .save()
```

### Processing multiple rows in a single call

By default, a single row is sent by the connector to your custom code. In many use cases, particularly when writing
documents, you will achieve far better performance when configuring the connector to send many rows in a single
call to your custom code.

The configuration option `spark.marklogic.write.batchSize` controls the number of row values sent to the custom code. 
If not specified, this defaults to 1 (as opposed to 100 when writing rows as documents). If set to a
value greater than one, then the values will be sent in the following manner:

1. If a custom schema is used, then the JSON objects representing the set of rows in the batch will first be added to a
   JSON array, and then the array will be set to the external variable.
2. Otherwise, the row values from the "URI" column will be concatenated together with a comma as a delimiter.


For approach #2, an alternate delimiter can be configured via `spark.marklogic.write.externalVariableDelimiter`. This
would be needed in case your "URI" values may have commas in them. Regardless of the delimiter value, you will 
typically use code like that shown below for splitting the "URI" value into many values:

```
for (var uri of URI.split(',')) {
  // Process each row value here. 
}
```

When using a custom schema, you will typically use [xdmp.fromJSON](https://docs.marklogic.com/xdmp.fromJSON) to convert
the value passed to your custom code into a JSON array:

```
// Assumes that URI is a JSON array node because a custom schema is being used. 
const array = fn.head(xdmp.fromJSON(URI));
```

Processing multiple rows in a single call can have a significant impact on performance by reducing the number of calls
to MarkLogic. For example, if you are writing documents with your custom code, it is recommended to try a batch size of
100 or greater to test how much performance improves. The 
[MarkLogic monitoring dashboard](https://docs.marklogic.com/guide/monitoring/dashboard) is a very useful tool for 
examining how many requests are being sent by the connector to MarkLogic and how quickly each request is processed, 
along with overall resource consumption.

### External variable configuration

As shown in the examples above, the custom code for processing a row must have an external variable named "URI". If 
your custom code declares an external variable with a different name, you can configure this via 
`spark.marklogic.write.externalVariableName`:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ['limit=10'], cts.collectionQuery('employee'))") \
    .load() \
    .write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.javascript", "console.log('Received value: ' + MY_VAR);") \
    .option("spark.marklogic.write.externalVariableName", "MY_VAR") \
    .mode("append") \
    .save()
```

### Custom external variables

You can pass additional external variables to your custom code by configuring one or more options with names starting with
`spark.marklogic.write.vars.`. The remainder of the option name will be used as the external variable name, and the value
of the option will be sent as the external variable value. Each external variable will be passed as a string due to
Spark capturing all option values as strings.

The following demonstrates two custom external variables being configured and used by custom JavaScript code:

```
spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ['limit=10'], cts.collectionQuery('employee'))") \
    .load() \
    .write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.vars.var1", "value1") \
    .option("spark.marklogic.write.vars.var2", "value2") \
    .option("spark.marklogic.write.javascript", "console.log('Received:', URI, var1, var2);") \
    .mode("append") \
    .save()
```

### Custom schema usage

If your dataset has a schema other than the default one expected by the connector - a single column named "URI" of 
type string - then the connector will convert the row into a JSON object before sending it to your custom code. 
The external variable in your custom code will then receive a JSON node as its value. In your custom code, you 
will typically use [xdmp.fromJSON](https://docs.marklogic.com/xdmp.fromJSON) to convert the value into a JSON object, 
allowing you to access its data:

```
// Assumes that URI is a JSON node because a custom schema is being used. 
const doc = fn.head(xdmp.fromJSON(URI));
```

### Streaming support

Spark's support for [streaming writes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
can be useful when you are already reading a stream of rows from MarkLogic because the query to fetch all matching
rows may take too long to execute. The connector allows you to then process each batch of results via custom code as
well. 

The following example is a variation of the example in the [reading guide for streaming rows](reading-data/optic.md). Instead
of using the connector's support for writing rows as documents, it shows each streamed batch being processed by custom
code:

```
import tempfile
stream = spark.readStream \
    .format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.partitions.javascript", "xdmp.databaseForests(xdmp.database('spark-example-content'))") \
    .option("spark.marklogic.read.javascript", "cts.uris(null, ['limit=10'], cts.collectionQuery('employee'), null, [PARTITION]);") \
    .load() \
    .writeStream \
    .format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.javascript", "console.log('Received URI: ' + URI);") \
    .option("checkpointLocation", tempfile.mkdtemp()) \
    .start()
stream.processAllAvailable()
stream.stop()
```


### Error handling

If the connector receives an error from MarkLogic when one or more rows are being processed via your custom code, 
the connector defaults to logging the error and asking Sparking to abort the entire write operation. To configure the 
connector to only log the error and continue writing batches of documents to MarkLogic, set the 
`spark.marklogic.write.abortOnFailure` option to a value of `false`. Similar to errors with reading data, the 
connector will strive to provide meaningful context when an error occurs to assist with debugging the cause of the error.

## Supported save modes

Spark supports 
[several save modes](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes) 
when writing data. The MarkLogic connector requires the `append` mode to be used. Because Spark defaults to 
the `error` mode, you will need to set this to `append` each time you use the connector to write data. 

`append` is the only supported mode due to MarkLogic not having the concept of a single "table" that a document 
must belong to. The Spark save modes give a user control over how data is written based 
on whether the target table exists. Because the concept of a rigid table does not exist in MarkLogic, the differences 
between the various modes do not apply to MarkLogic. Note that while a collection in MarkLogic has some similarities to 
a table, it is fundamentally different in that a document can belong to zero to many collections and collections do not
impose any schema constraints. 
