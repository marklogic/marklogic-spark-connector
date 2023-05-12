---
layout: default
title: Getting Started with PySpark
nav_order: 3
---

This section describes how to use the MarkLogic Spark connector with 
[PySpark](https://spark.apache.org/docs/latest/api/python/index.html). Instructions for using a Spark connector will
vary across Spark environments. This guide is intended to show how to use the MarkLogic Spark connector in a 
common Spark environment to assist with getting the connector working in your desired Spark environment.

## Install PySpark

The [PySpark installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) describes
how to install PySpark. As noted in that guide, you will need to install Python 3 first if you do not already have it 
installed. [pyenv](https://github.com/pyenv/pyenv#installation) is recommended for doing so, as it simplifies 
installing multiple versions of Python and easily switching between them. You are free though to install Python 3 in 
any manner you wish.

Once you have installed PySpark, run the following from a command line to ensure PySpark is installed correctly:

    pyspark

This should open a Python shell and print logging stating that Spark is available to be used. Exit out of this 
shell by pressing `ctrl-D`. 


## Obtaining the connector

The MarkLogic Spark connector can be downloaded from 
[this repository's Releases page](https://github.com/marklogic/marklogic-spark-connector/releases). The instructions 
below will assume that you have downloaded the connector file to your local home directory.

TODO - until the 1.0 release occurs, you will need to build the Spark connector yourself (this will be removed 
before the 1.0 release occurs). To do so:

- Clone this repository.
- Run `./gradlew clean shadowJar`.

The connector can then be accessed at `./build/libs/marklogic-spark-connector-1.0-SNAPSHOT.jar`.

## Deploy an example application 

The MarkLogic Spark connector allows a user to specify an 
[Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) to select rows to retrieve from 
MarkLogic. The query depends on a [MarkLogic view](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_68685) that 
projects data from documents in MarkLogic into rows. 

To facilitate using the connector with PySpark, perform the following steps to deploy an example application to your
MarkLogic server that includes a 
[TDE view](https://docs.marklogic.com/guide/app-dev/TDE) and some documents that conform to that view.

1. Clone this repository using git. (TODO Should we provide the example project as a downloadable zip?) 
2. In the directory in which you cloned this repository, run `cd examples/pyspark`. 
3. Create a file named `gradle-local.properties` and add `mlPassword=changeme`, changing the text "changeme" to the 
   password of your MarkLogic `admin` user. 
4. Open the `gradle.properties` file and verify that the value of the `mlPort` property is an available port on the 
   machine running your MarkLogic server; the default port is 8020. 
5. Run `./gradlew -i mlDeploy` to deploy the example application.

After the deployment finishes, your MarkLogic server will now have the following:

- An app server named `pyspark-example` listening on port 8020 (or the port you chose if you overrode the `mlPort` 
  property).
- A database named `pyspark-example-content` that contains 1000 JSON documents in the `employee` collection.
- A TDE with a schema name of `example` and a view name of `employee`.
- A user named `pyspark-example-user` that can be used with the Spark connector and in MarkLogic's qconsole tool.

To verify that your application was deployed correctly, access your MarkLogic server's qconsole tool - for example, 
if your MarkLogic server is deployed locally, you will go to http://localhost:8000/qconsole . Then perform the 
following steps:

1. In the "Database" dropdown, select `pyspark-example-content`.
2. In the "Query Type" dropdown, select `Optic DSL`.
3. Enter the following query into an editor in qconsole: `op.fromView('example', 'employee').limit(10)`.
4. Click on the "Run" button. This should display 10 JSON objects, each being a projection of a row from an employee 
   document in the database.

## Using the connector

Run PySpark from the directory that you downloaded the connector to:

    pyspark --jars marklogic-spark-connector-1.0.0.jar

The `--jars` command line option is PySpark's method for utilizing Spark connectors. Each Spark environment should have
a similar mechanism for including third party connectors; please see the documentation for your particular Spark 
environment. In the example above, the `--jars` option allows for the MarkLogic Spark connector to be used within 
PySpark. 

### Reading data with the connector

The connector reads data from MarkLogic as rows to construct a Spark DataFrame. To see this in action, 
paste the following Python statement into PySpark, adjusting the host and password values as needed:

```
df = spark.read.format("com.marklogic.spark")\
    .option("spark.marklogic.client.host", "localhost")\
    .option("spark.marklogic.client.port", "8020")\
    .option("spark.marklogic.client.username", "pyspark-example-user")\
    .option("spark.marklogic.client.password", "password")\
    .option("spark.marklogic.read.opticDsl", "op.fromView('example', 'employee')")\
    .load()
```

When using `digest` or `basic` authentication, you can also use this more succinct approach for specifying the 
client options in one option:

```
df = spark.read.format("com.marklogic.spark")\
    .option("spark.marklogic.client.uri", "pyspark-example-user:password@localhost:8020")\
    .option("spark.marklogic.read.opticDsl", "op.fromView('example', 'employee')")\
    .load()
```

The `df` variable is an instance of a Spark DataFrame. Try the following commands on it:

    df.count()
    df.head()
    df.show(10)

The [PySpark docs](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) provide more 
information on how a Spark DataFrame works along with more commands that you can try on it. 

The instructions above can be applied to your own MarkLogic application. You can use the same Spark command above, 
simply adjusting the connection details and the Optic DSL query. Please see [the guide on reading](reading.md) for 
more information on how data can be read from MarkLogic.

### Writing data to the connector

The connector writes the rows in a Spark DataFrame to MarkLogic as new JSON documents, which can also be transformed 
into XML documents if desired. To try this on the DataFrame that was read from MarkLogic in the above section, 
paste the following into PySpark, adjusting the host and password values as needed:

```
df.write.format("com.marklogic.spark")\
    .option("spark.marklogic.client.host", "localhost")\
    .option("spark.marklogic.client.port", "8020")\
    .option("spark.marklogic.client.username", "pyspark-example-user")\
    .option("spark.marklogic.client.password", "password")\
    .option("spark.marklogic.write.collections", "write-test")\
    .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update")\
    .option("spark.marklogic.write.uriPrefix", "/write/")\
    .mode("append")\
    .save()
```

To examine the results, access your MarkLogic server's qconsole tool again and click on the "Explore" button for the 
`pyspark-example-content` database. The database should now have 2,000 documents - the 1,000 documents in the 
`employee` collection that were loaded when the application was deployed, and the 1,000 documents in the 
`write-test` collection that were written by the PySpark command above. Each document in the `write-test` collection 
will have field names based on the column names in the Spark DataFrame. 

For more information on writing data to MarkLogic, see the [guide on writing data](writing.md).

