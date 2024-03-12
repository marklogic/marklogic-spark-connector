---
layout: default
title: Aggregate XML
parent: Reading files
grand_parent: Reading Data
nav_order: 2
---

XML files often contain aggregate data that can be disaggregated by splitting it into
multiple smaller documents rooted at a recurring element. Disaggregating large XML files consumes fewer resources
during loading and improves performance when searching and retrieving content. This guide describes how to use the
connector to read aggregate XML files and produce many rows from specific child elements.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Usage

The connector supports the above use case via the `spark.marklogic.read.aggregates.xml.element` and optional 
`spark.marklogic.read.aggregates.xml.namespace` options. When using these options, the connector will return rows with
the same schema as used by 
[Spark's Binary data source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html). The connector
knows how to write rows adhering to this schema as documents in MarkLogic. 

The `examples/getting-started` directory in this repository contains a small XML file with multiple occurrences of 
the element `Employee` in the namespace `org:example`. The following command demonstrates how to read this file such
that each occurrence of the element `Employee` becomes a separate row in Spark (note that the namespace option is
not required):

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.aggregates.xml.element", "Employee") \
  .option("spark.marklogic.read.aggregates.xml.namespace", "org:example") \
  .load("data/employees.xml")
df.show()
```

You can then write each of the rows as separate XML documents in MarkLogic:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.aggregates.xml.element", "MedlineCitation") \
  .option("spark.marklogic.read.aggregates.xml.uriElement", "MedlineID") \
  .load("/Users/rudin/data/medline02n0228.xml")

df.write.format("marklogic") \
  .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
  .option("spark.marklogic.write.collections", "aggregate-xml") \
  .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
  .option("spark.marklogic.write.uriReplace", ".*/data,'/xml'") \
  .mode("append") \
  .save()
```

The above will produce 3 XML documents, each with a root element of `Employee` in the `org:example` namespace`, in the
`aggregate-xml` collection in MarkLogic.

## Generating a URI via an element

Some XML documents may contain a particular element that is useful for generating a unique URI for each document. 
You can specify the element name and optional namespace via the `spark.marklogic.read.aggregates.xml.uriElement` and
optional `spark.marklogic.read.aggregates.xml.uriNamespace` options as shown below:

```
df = spark.read.format("marklogic") \
  .option("spark.marklogic.read.aggregates.xml.element", "Employee") \
  .option("spark.marklogic.read.aggregates.xml.namespace", "org:example") \
  .option("spark.marklogic.read.aggregates.xml.uriElement", "name") \
  .option("spark.marklogic.read.aggregates.xml.uriNamespace", "org:example") \
  .load("data/employees.xml")
df.show()
```

## Reading compressed files

The connector supports reading GZIP and ZIP compressed files via the `spark.marklogic.read.files.compression` option.

For a GZIP compressed file, set the option to a value of `gzip`:

```
.option("spark.marklogic.read.files.compression", "gzip")
```

Each aggregate XML file will be gunzipped first and then processed normally. 

For a ZIP compressed file, which can contain one to many aggregate XML files, set the option to a value of `zip`:

```
.option("spark.marklogic.read.files.compression", "zip")
```

Each entry in the zip file must be an aggregate XML file. The same element and namespace, along with URI element and
namespace, will be applied to every file in the zip. 

## Error handling

The connector defaults to throwing any error that occurs while reading an aggregate XML file. You can set the 
`spark.marklogic.read.files.abortOnFailure` option to `false` to have each error logged instead. The connector will 
continue trying to process each aggregate XML file.

In the case of an error due to an element missing the child element specified by 
`spark.marklogic.read.aggregates.xml.uriElement`, the connector will log the error and continue trying to process 
elements in the aggregate XML file. 
