---
layout: default
title: RDF data
parent: Reading files
grand_parent: Reading Data
nav_order: 3
---

The MarkLogic connector supports reading [files containing RDF data](https://www.w3.org/RDF/), allowing you to ingest
this data into MarkLogic and leverage the power of 
[semantic graphs in MarkLogic](https://docs.marklogic.com/guide/semantics). 

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Reading RDF files

To read RDF files, configure the connector by setting the option `spark.marklogic.read.files.type` to a value of `rdf`:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.type", "rdf") \
  .load("data/taxonomy.xml")
df.show()
```

The connector returns rows with the following schema:

- `subject` = string representing the subject of the RDF triple.
- `predicate` = string representing the predicate of the RDF triple.
- `object` = string representing the object value of the RDF triple.
- `datatype` = optional string defining datatype URI of the literal value in the `object` column.
- `lang` = optional string defining the language of the literal value in the `object` column.
- `graph` = optional string defining the graph associated with the RDF triple; only populated when reading quads.

When reading files containing quads, such as TriG and N-Quads files, the `graph` column will be populated with the
semantic graph associated with each triple:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.type", "rdf") \
  .load("data/quads.trig")
df.show()
```

## Supported RDF file types

The connector supports the same [RDF data formats](https://docs.marklogic.com/guide/semantics/loading#id_70682) as 
MarkLogic server does, which are listed below:

- RDF/JSON
- RDF/XML
- N3
- N-Quads
- N-Triples
- TriG
- Turtle

The connector depends on the [Apache Jena library](https://jena.apache.org/index.html) for reading RDF files. While the
connector has only been officially tested with the above files types, you may be able to read all the
[supported Jena file types](https://jena.apache.org/documentation/io/#command-line-tools).

## Configuring a graph

The connector supports configuring a graph when writing triples to MarkLogic, but not yet when reading data from RDF
files. However, you can easily set the value of the `graph` column in each row via Spark - one approach is shown below:

```
from pyspark.sql.functions import lit
spark.read.format("marklogic") \
  .option("spark.marklogic.read.files.type", "rdf") \
  .load("data/taxonomy.xml") \
  .withColumn("graph", lit("example-graph")) \
  .show()
```

## Reading compressed files

The connector supports reading GZIP and ZIP compressed files via the `spark.marklogic.read.files.compression` option.

For a GZIP compressed RDF file, set the option to a value of `gzip`:

```
.option("spark.marklogic.read.files.compression", "gzip")
```

Each RDF file will be unzipped first and then processed normally.

For a ZIP compressed file, which may contain one to many RDF files, set the option to a value of `zip`:

```
.option("spark.marklogic.read.files.compression", "zip")
```

The ZIP may contain RDF files of different types. Each RDF file will be processed separately, with each triple in 
each file becoming a separate Spark row. 


## Error handling

The connector defaults to throwing any error that occurs while reading an RDF file. You can set the
`spark.marklogic.read.files.abortOnFailure` option to `false` to have each error logged instead. When an error occurs,
the connector will not process the rest of the file that caused the error, but it will continue processing every other
selected file or entry in a zip file. 

