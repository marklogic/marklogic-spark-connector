---
layout: default
title: Reading files
nav_order: 4
has_children: true
parent: Reading Data
---


As of the 2.3.0 release, the MarkLogic Spark connector extends the out-of-the-box Spark capabilities for reading files
to include support for reading the following file types:

- Aggregate XML files
- RDF files
- ZIP files

If you wish to read and write files that are not one of the above types, you can use the 
[Spark Binary data source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html). The connector will
recognize the schema of rows read with the Binary data source and will write one document per row. 

Please see the guides below for more information on each of the above file type.
