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

* Apache Spark 3.5.6 is recommended, but earlier versions of Spark 3.5.x, 3.4.x, and 3.3.x should work as well. When choosing 
[a Spark distribution](https://spark.apache.org/downloads.html), you must select a distribution that uses Scala 2.12 and not Scala 2.13.
* For writing data, MarkLogic 9.0-9 or higher.
* For reading data, MarkLogic 10.0-9 or higher.
* If using Java to run Spark, Java 11 or higher is required.

In addition, if your MarkLogic cluster has multiple hosts in it, it is highly recommended to put a load balancer in front
of your cluster and have the MarkLogic Spark connector connect through the load balancer. 

Please see the [Getting Started guide](getting-started/getting-started.md) to begin using the connector. 
