---
layout: default
title: Setup
parent: Getting Started
nav_order: 1
---

The instructions below should be followed before attempting any of the examples in the guides for specific Spark 
environments, as those examples depend on an application being deployed to MarkLogic.

## Obtaining the connector

The MarkLogic Spark connector can be downloaded from
[this repository's Releases page](https://github.com/marklogic/marklogic-spark-connector/releases). Each Spark 
environment should have documentation on how to include third-party connectors; please consult your Spark 
environment's documentation on how to achieve this.


## Deploy an example application

The connector allows a user to specify an
[Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) to select rows to retrieve from
MarkLogic. The query depends on a [MarkLogic view](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_68685) that
projects data from documents in MarkLogic into rows.

To facilitate trying out the connector, perform the following steps to deploy an example application to your
MarkLogic server that includes a
[TDE view](https://docs.marklogic.com/guide/app-dev/TDE) and some documents that conform to that view.

1. From [this repository's Releases page](https://github.com/marklogic/marklogic-spark-connector/releases), select 
   the latest release and download the `marklogic-spark-getting-started-2.0.0.zip` file.
2. Extract the contents of the downloaded zip file. 
3. Open a terminal window and go to the directory created by extracting the zip file; the directory should have a 
   name of "marklogic-spark-getting-started-2.0.0".
4. Create a file named `gradle-local.properties` and add `mlPassword=changeme`, changing the text "changeme" to the
   password of your MarkLogic `admin` user.
5. Open the `gradle.properties` file and verify that the value of the `mlPort` property is an available port on the
   machine running your MarkLogic server; the default port is 8020.
6. Ensure that the `./gradlew` file is executable; depending on your operating system, you may need to run
   `chmod 755 gradlew` to make the file executable.
7. Run `./gradlew -i mlDeploy` to deploy the example application.

After the deployment finishes, your MarkLogic server will now have the following:

- An app server named `spark-example` listening on port 8020 (or the port you chose if you overrode the `mlPort`
  property).
- A database named `spark-example-content` that contains 1000 JSON documents in the `employee` collection.
- A TDE with a schema name of `example` and a view name of `employee`.
- A user named `spark-example-user` that can be used with the Spark connector and in MarkLogic's qconsole tool.

To verify that your application was deployed correctly, access your MarkLogic server's qconsole tool - for example,
if your MarkLogic server is deployed locally, you will go to http://localhost:8000/qconsole . You can authenticate as 
the `spark-example-user` user that was created above, as it's generally preferable to test as a non-admin user. 
After authenticating, perform the following steps:

1. In the "Database" dropdown, select `spark-example-content`.
2. In the "Query Type" dropdown, select `Optic DSL`.
3. Enter the following query into an editor in qconsole: `op.fromView('example', 'employee').limit(10)`.
4. Click on the "Run" button. This should display 10 JSON objects, each being a projection of a row from an employee
   document in the database.
