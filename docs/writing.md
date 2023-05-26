---
layout: default
title: Writing Data
nav_order: 6
---

The MarkLogic Spark connector allows for writing rows in a Spark DataFrame to MarkLogic as documents. The sections below
provide more detail about how this process works and how it can be controlled.

## Controlling document content

Rows in a Spark DataFrame are written to MarkLogic by default as JSON documents. Each column in a row becomes a 
top-level field in the JSON document. 

To change the content of documents, a [REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) can be 
configured via the `spark.marklogic.write.transform` option. The transform will receive a JSON document as the 
document content. This can be transformed in any manner, including into XML documents. For example, the 
[transform-from-json](https://docs.marklogic.com/json:transform-from-json) MarkLogic function could be used to 
convert the JSON document into an XML document, which then can be further modified by the code in the REST transform. 

Parameters can be passed to your REST transform via the `spark.marklogic.write.transformParams` option. The value of 
this option must be a comma-delimited string of the form `param1,value1,param2,value,etc`. For example, if your 
transform accepts parameters named "color" and "size", the following option would pass values to the transform for 
those parameter names:

    .option("spark.marklogic.write.transform", "my-transform")
    .option("spark.marklogic.write.transformParams", "color,blue,size,medium")

If one of your parameter values has a comma in it, you can change the delimiter via the 
`spark.marklogic.write.transformParamsDelimiter` option. The following options show how this would be used if one of 
the parameter values contains a comma:

    .option("spark.marklogic.write.transform", "my-transform")
    .option("spark.marklogic.write.transformParams", "my-param;has,commas")
    .option("spark.marklogic.write.transformParamsDelimiter", ";")

## Configuring document URIs

By default, the connector will construct a URI for each document beginning with a UUID and ending with `.json`. A 
prefix can be specified via `spark.marklogic.write.uriPrefix`, and the default suffix of `.json` can be overridden 
via `spark.marklogic.write.uriSuffix`. For example, the following options would results in URIs of the form 
"/employee/(a random UUID value)/record.json":

    .option("spark.marklogic.write.uriPrefix", "/employee/")
    .option("spark.marklogic.write.uriSuffix", "/record.json")

URIs can also be constructed based on column values for a given row. The `spark.marklogic.write.uriTemplate` option 
allows for column names to be referenced via braces when constructing a URI. Additionally, if this option is used, the 
above options for setting a prefix and suffix will be ignored, as the template can be used to define the entire URI. 

For example, consider a Spark DataFrame with, among other columns, columns named `organization` and `employee_id`. 
The following template would construct URIs based on both columns:

    .option("spark.marklogic.write.uriTemplate", "/example/{organization}/{employee_id}.json")

Both columns should have values in each row in the DataFrame. If the connector encounters a row that does not have a 
value for any column in the URI template, an error will be thrown.

## Configuring document metadata

Each document written by the connector can be assigned to zero to many 
[collections in MarkLogic](https://docs.marklogic.com/guide/search-dev/collections). Collections are specified as a 
comma-delimited list via the `spark.marklogic.write.collections` option. For example, the following will assign each 
document to collections named `employee` and `data`:

    .option("spark.marklogic.write.collections", "employee,data")

Each document can also be assigned to zero to many 
[permissions in MarkLogic](https://docs.marklogic.com/guide/security/permissions). Generally, you will want to 
assign at least one read permission and one update permission so that at least some users of your application can 
read and update the documents. 

Permissions are specified as a comma-delimited list via the `spark.marklogic.write.permissions` option. The list is 
a series of MarkLogic role names and capabilities in the form `role1,capability1,role2,capability2,etc`. For example,
the following will assign each document a read permission for the role `rest-reader` and an update permission for 
the role `rest-writer`:

    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update")

If you are using [MarkLogic's support for temporal data](https://docs.marklogic.com/guide/temporal/intro), you can 
also specify a temporal collection for each document to be assigned to via the 
`spark.marklogic.write.temporalCollection`. Each document must define values for the axes associated with the 
temporal collection. 

## Streaming support

The MarkLogic Spark connector supports 
[streaming writes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) to MarkLogic. 
The connector configuration does not change; instead, different Spark APIs are used to read a stream of data and 
write that stream to MarkLogic. 

A common use case for streaming involves writing data to MarkLogic from a CSV file, where there is no need to capture
the data in a DataFrame first; the data just needs to be written to MarkLogic. The following shows an example in 
PySpark of streaming CSV files from a directory (it also uses the `spark.marklogic.client.uri` option for a more 
concise set of options). This example can be run from the `./examples/pyspark` directory in this repository after 
following the instructions in the [Getting Started with PySpark guide](getting-started-pyspark.md) for deploying the 
example application:

```
import tempfile
from pyspark.sql.types import *
spark.readStream \
    .format("csv") \
    .schema(StructType([StructField("GivenName", StringType()), StructField("Surname", StringType())])) \
    .option("header", True) \
    .load("data/csv-files") \
    .writeStream \
    .format("com.marklogic.spark") \
    .option("checkpointLocation", tempfile.mkdtemp()) \
    .option("spark.marklogic.client.uri", "pyspark-example-user:password@localhost:8020") \
    .option("spark.marklogic.write.uriPrefix", "/streaming-example/") \
    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
    .option("spark.marklogic.write.collections", "streaming-example") \
    .start() \
    .processAllAvailable()
```

The above example will stream the data in the `./data/csv-files/100-employees.csv` file through the MarkLogic Spark 
connector and into MarkLogic. This will result 100 new JSON documents in the `streaming-example` collection. 

The ability to stream data into MarkLogic can make Spark an effective tool for obtaining data from a variety of data 
sources and writing it directly to MarkLogic. 

## Error handling

The connector may throw an error during one of two phases of operation - before it begins to write data to MarkLogic, 
and during the writing of data to MarkLogic. 

For the first kind of error, the error will be immediately returned to the user and no data will have been written. 
Such errors are often due to misconfiguration of the connector options and should be fixable. 

For the second kind of error, the error will eventually be returned to the user, usually within seconds of it 
occurring. The slight delay is due to the asynchronous nature of data being written by the connector. The error will 
be logged by the connector and the write operation will be aborted. Any batches of documents that were written 
successfully prior to the error occurring will still exist in the database. 

Similar to errors with reading data, the connector will strive to provide meaningful context when an error occurs to 
assist with debugging the cause of the error. Any errors that cannot be fixed via changes to the options passed to the 
connector should be reported as new issues to this GitHub repository.

## Tuning performance

The MarkLogic Spark connector uses MarkLogic's 
[Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement) for writing documents to a database. The 
following options can be set to adjust how the connector performs when writing data:

- `spark.marklogic.write.batchSize` = the number of documents written in one call to MarkLogic; defaults to 100
- `spark.marklogic.write.threadCount` = the number of threads used by each partition to write documents to MarkLogic;
  defaults to 4

These options are in addition to the number of partitions within the Spark DataFrame that is being written to 
MarkLogic. For each partition in the DataFrame, a separate instance of a MarkLogic batch writer is created, each 
with its own set of threads. 

Optimizing performance will thus involve testing various combinations of partition counts, batch sizes, and thread 
counts. The [MarkLogic Monitoring tools](https://docs.marklogic.com/guide/monitoring/intro) can help you understand 
resource consumption and throughput from Spark to MarkLogic. 

## Supported save modes

Spark supports 
[several save modes](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes) 
when writing data. The MarkLogic Spark connector requires the `append` mode to be used. Because Spark defaults to 
the `error` mode, you will need to set this to `append` each time you use the connector to write data. 

`append` is the only supported mode because MarkLogic does not have the concept of a "table" that a document 
must belong to, and only belong to one of. The Spark save modes give a user control over how data is written based 
on whether the target table exists. Because no such concept of a table exists in MarkLogic, the differences between 
the various modes do not apply to MarkLogic. Note that while a collection in MarkLogic has some similarities to a 
table, it is fundamentally different in that a document can belong to zero to many collections. 
