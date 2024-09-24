---
layout: default
title: Generic file support
parent: Reading files
grand_parent: Reading Data
nav_order: 1
---

The MarkLogic connector extends Spark's support for reading files to include file types that benefit from special 
handling when trying to import files into MarkLogic. This page describes the features that are inherited from 
Spark for reading files.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Selecting files to read

Use Spark's standard `load()` function or `path` option:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.compression", "zip") \
  .load("path/to/zipfiles")
```

Or:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.compression", "zip") \
  .option("path", "path/to/zipfiles") \
  .load()
```

## Generic Spark file source options

The connector also supports the following 
[generic Spark file source options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html):

- Use `pathGlobFilter` to only include files with file names matching the given pattern.
- Use `recursiveFileLookup` to include files in child directories.
- Use `modifiedBefore` and `modifiedAfter` to select files based on their modification time.

## Reading and writing large binary files

The 2.3.2 connector introduces a fix for reading and writing large binary files to MarkLogic, allowing for the contents
of each file to be streamed from its source to MarkLogic. This avoids an issue where the Spark environment runs out
of memory while trying to fit the contents of a file into an in-memory row. 

To enable this, include the following in the set of options passed to your reader:

    .option("spark.marklogic.files.stream", "true")

As a result of this option, the `content` column in each row will not contain the contents of the file. Instead, 
it will contain a serialized object intended to be used during the write phase to read the contents of the file as a 
stream. 

Files read from the MarkLogic Spark connector with the above option can then be written as documents to MarkLogic 
with the same option above being passed to the writer. The connector will then stream the contents of each file to
MarkLogic, submitting one request to MarkLogic per document. 

## Reading any file

If you wish to read files without any special handling provided by the connector, you can use the
[Spark Binary data source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html). If you try to write these rows as documents, the connector will recognize
the Binary data source schema and write each row as a separate document. For example, the following will 
write each file in the `examples/getting-started/data` directory in this repository without any special handling
of each file:

```
spark.read.format("binaryFile") \
  .option("recursiveFileLookup", True) \
  .load("data") \
  .write.format("marklogic") \
  .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
  .option("spark.marklogic.write.collections", "binary-example") \
  .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
  .option("spark.marklogic.write.uriReplace", ".*data,'/binary-example'") \
  .mode("append") \
  .save()
```

The above will result in each document in the `data` directory being written as a document to MarkLogic. MarkLogic
will determine the document type based on the file extension. 

If you are writing files with extensions that MarkLogic does not recognize based on its configured set of MIME types, 
you can force a document type for each file with an unrecognized extension:

```
  .option("spark.marklogic.write.fileRows.documentType", "JSON")
```

The `spark.marklogic.write.fileRows.documentType` option supports values of `JSON`, `XML`, and `TEXT`. 

Please see [the guide on writing data](../../writing.md) for information on how "file rows" can then be written to
MarkLogic as documents.
