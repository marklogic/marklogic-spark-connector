---
layout: default
title: Writing Data
nav_order: 5
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

    .options("spark.marklogic.write.uriTemplate", "/example/{organization}/{employee_id}.json")

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
