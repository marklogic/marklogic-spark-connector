---
layout: default
title: Setup
parent: Getting Started
nav_order: 1
---

The instructions below should be followed before attempting any of the examples in the guides for specific Spark 
environments, as those examples depend on an application being deployed to MarkLogic.

## Obtaining the connector

The MarkLogic connector can be downloaded from
[this repository's Releases page](https://github.com/marklogic/marklogic-spark-connector/releases). Each Spark 
environment should have documentation on how to include third-party connectors; please consult your Spark 
environment's documentation on how to achieve this.


## Deploy an example application

The connector allows a user to specify an
[Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) to select rows to retrieve from
MarkLogic. The query depends on a [MarkLogic view](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_68685) that
projects data from documents in MarkLogic into rows.

To facilitate trying out the connector, perform the following steps to deploy an example application to your
MarkLogic server that includes a
[TDE view](https://docs.marklogic.com/guide/app-dev/TDE) and some documents that conform to that view. These instructions depend on 
[using Docker](https://docs.docker.com/get-docker/) to install and initialize an instance of MarkLogic. If you already
have an instance of MarkLogic running, you can skip step 4 below, but ensure that the `gradle.properties` file in the
extracted directory contains valid connection properties for your instance of MarkLogic.

1. From [this repository's Releases page](https://github.com/marklogic/marklogic-spark-connector/releases), select 
   the latest release and download the `marklogic-spark-getting-started-2.5.0.zip` file.
2. Extract the contents of the downloaded zip file. 
3. Open a terminal window and go to the directory created by extracting the zip file; the directory should have a 
   name of "marklogic-spark-getting-started-2.5.0".
4. Run `docker-compose up -d` to start an instance of MarkLogic
5. Ensure that the `./gradlew` file is executable; depending on your operating system, you may need to run
   `chmod 755 gradlew` to make the file executable.
6. Run `./gradlew -i mlDeploy` to deploy the example application.

After the deployment finishes, your MarkLogic server will now have the following:

- An app server named `spark-example` listening on port 8003.
- A database named `spark-example-content` that contains 1000 JSON documents in a collection named `employee`.
- A TDE with a schema name of `example` and a view name of `employee`.
- A user named `spark-example-user` with a password of `password` that can be used with the Spark connector and [MarkLogic's qconsole tool](https://docs.marklogic.com/guide/qconsole/intro).

To verify that your application was deployed correctly, access your MarkLogic server's qconsole tool 
via <http://localhost:8000/qconsole> . You can authenticate as the `spark-example-user` user that was created above, 
as it's generally preferable to test as a non-admin user. 

After authenticating, perform the following steps:

1. In the "Database" dropdown, select `spark-example-content`.
2. In the "Query Type" dropdown, select `Optic DSL`.
3. Enter the following query into an editor in qconsole: `op.fromView('example', 'employee').limit(10)`.
4. Click on the "Run" button. This should display 10 JSON objects, each being a projection of a row from an employee
   document in the database.
