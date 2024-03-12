---
layout: default
title: ZIP files
parent: Reading files
grand_parent: Reading Data
nav_order: 4
---

The MarkLogic connector has special handling for ZIP files, enabling each entry in a ZIP file to be read as a separate
row and eventually written to MarkLogic as a separate document. 

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Specifying ZIP files to read

To configure the connector to read each entry in one or more ZIP files as separate rows, set the 
`spark.marklogic.read.files.compression` option to a value of `zip`:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.compression", "zip") \
  .load("data/employees.zip")
df.show()
```

The connector will return 1 row per entry in each zip file, with each row conforming to the 
[Spark Binary data source schema](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html). Each row 
will have a `path` column with a value based on the path of the ZIP file and the name of the ZIP entry.

To see the full path - which you will likely want to customize if writing these rows as documents to 
MarkLogic - try the following:

```
df.select("path").show(20, 0, True)
```

Please see [the guide on writing data](../../writing.md) for information on how "file rows" can then be written to 
MarkLogic as documents.

## Error handling

Due to how the underlying Java support for reading ZIP files works, files that are not valid ZIP files do not result
in any errors being thrown. Instead, the Java support simply does not return any rows for any file that it cannot read
as a ZIP file. 
