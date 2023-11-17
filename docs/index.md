---
layout: default
title: Introduction
nav_order: 1
---

The MarkLogic connector for Apache Spark is an [Apache Spark 3 connector](https://spark.apache.org/docs/latest/) that supports
reading data from and writing data to MarkLogic. Within any Spark 3 environment, the connector enables users to easily 
query for data in MarkLogic, manipulate it using widely-known Spark operations, and then write results back to 
MarkLogic or disseminate them to another system. Data can also be easily imported into MarkLogic by first reading it 
from any data source that Spark supports and then writing it to MarkLogic.

The connector has the following system requirements:

* Apache Spark 3.3.0 or higher. The connector has been tested with the latest versions of Spark 3.3.x of 3.4.x.
* For writing data, MarkLogic 9.0-9 or higher.
* For reading data, MarkLogic 10.0-9 or higher.

Please see the [Getting Started guide](getting-started/getting-started.md) to begin using the connector. 
