---
layout: default
title: PySpark
parent: Getting Started
nav_order: 2
---

[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) is a Python API for Spark and an excellent choice
for learning how to use Spark. This guide describes how to install PySpark and use it with the MarkLogic connector. 

Before going further, be sure you've followed the instructions in the [Getting Started](getting-started.md) guide for
obtaining the connector and deploying an example application to MarkLogic.

## Install PySpark

The [PySpark installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) describes
how to install PySpark. As noted in that guide, you will need to install Python 3 first if you do not already have it
installed. [pyenv](https://github.com/pyenv/pyenv#installation) is a popular choice for doing so, as it simplifies
installing and switching between multiple versions of Python. Additionally, be sure to select a PySpark installation 
that depends on Scala 2.12 and not Scala 2.13.

Once you have installed PySpark, run the following from a command line to ensure PySpark is installed correctly:

    pyspark

This should open a Python shell and print logging stating that Spark is available to be used. Exit out of this
shell by pressing `ctrl-D`.

## Using the connector

Run PySpark from the directory that you downloaded the connector to per the [setup instructions](setup.md):

    pyspark --jars marklogic-spark-connector-2.4.2.jar

The `--jars` command line option is PySpark's method for utilizing Spark connectors. Each Spark environment should have
a similar mechanism for including third party connectors; please see the documentation for your particular Spark
environment. In the example above, the `--jars` option allows for the connector to be used within
PySpark.

When PySpark starts, you should see information like this on how to configure logging:

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

Setting the default log level to `INFO` or `DEBUG` will show logging from the MarkLogic connector. This will also
include potentially significant amounts of log messages from PySpark itself.

### Reading data with the connector

The connector reads data from MarkLogic as rows to construct a Spark DataFrame. To see this in action,
paste the following Python statement into PySpark, adjusting the host and password values as needed:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.host", "localhost") \
    .option("spark.marklogic.client.port", "8003") \
    .option("spark.marklogic.client.username", "spark-example-user") \
    .option("spark.marklogic.client.password", "password") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load()
```

When using `digest` or `basic` authentication, you can also use this more succinct approach for specifying the
client options in one option:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')") \
    .load()
```

The `df` variable is an instance of a Spark DataFrame. Try the following commands on it:

    df.count()
    df.head()
    df.show(10)

The [PySpark docs](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) provide more
information on how a Spark DataFrame works along with more commands that you can try on it.

As of the connector 2.2.0 release, you can also query for documents, receiving "document" rows that contain columns
capturing the URI, content, and metadata for each document:

```
df = spark.read.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.read.documents.collections", "employee") \
    .load()
df.show()
```

The instructions above can be applied to your own MarkLogic application. You can use the same Spark command above,
simply adjusting the connection details and the Optic query. Please see 
[the guide on reading data](../reading-data/reading.md) for more information on how data can be read from MarkLogic, 
including both via search queries and via custom JavaScript and XQuery code.

### Writing data to the connector

The connector writes the rows in a Spark DataFrame to MarkLogic as new JSON documents, which can also be transformed
into XML documents. To try this on the DataFrame that was read from MarkLogic in the above section,
paste the following into PySpark, adjusting the host and password values as needed:

```
df.write.format("marklogic") \
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003") \
    .option("spark.marklogic.write.collections", "write-test") \
    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update") \
    .option("spark.marklogic.write.uriPrefix", "/write/") \
    .mode("append") \
    .save()
```

To examine the results, access your [MarkLogic server's qconsole tool](https://docs.marklogic.com/guide/qconsole/intro) 
and click on the "Explore" button for the `spark-example-content` database. The database should now have 
2,000 documents - the 1,000 documents in the
`employee` collection that were loaded when the application was deployed, and the 1,000 documents in the
`write-test` collection that were written by the PySpark command above. Each document in the `write-test` collection
will have field names based on the column names in the Spark DataFrame.

For more information on writing data to MarkLogic, see the [guide on writing data](../writing.md).
