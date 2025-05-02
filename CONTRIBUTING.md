This guide covers how to develop and test this project. It assumes that you have cloned this repository to your local
workstation.

**You must use Java 17 for developing, testing, and building this project**, even though the connector supports
running on Java 11. For users, Java 17 is only required if using the splitting and embedding features, as those
depend on a third party module that requires Java 17.

**You also need Java 11 installed** so that the subprojects in this repository that require Java 11 have access to a 
Java 11 SDK. [sdkman](https://sdkman.io/) is highly recommend for installing multiple JDKs.

# Setup

To begin, you need to deploy the test application in this project to MarkLogic. You can do so either on your own 
installation of MarkLogic, or you can use `docker compose` to install MarkLogic, optionally as a 3-node cluster with 
a load balancer in front of it.

## Installing MarkLogic with docker compose

If you wish to use `docker compose`, perform the following steps before deploying the test application.

1. [Install Docker](https://docs.docker.com/get-docker/).
2. Ensure that you don't have a MarkLogic instance running locally (if you do, you may run into port conflicts in 
   the next step).
3. Run `docker compose up -d --build`.

The above will result in a new MarkLogic instance with a single node. 

## Deploying the test application

To deploy the test application, first create `./gradle-local.properties` and add the following to it:

    mlPassword=the password of your admin user

Then deploy the test application:

    ./gradlew -i mlDeploy

After the deployment finishes, you can go to http://localhost:8016 to verify that you get the MarkLogic REST API 
index page for the test application server. 

# Running the tests

To run the tests against the test application, run the following Gradle task:

    ./gradlew test

**To run the tests in Intellij**, you must configure your JUnit template to include a few JVM args:

1. Go to Run -> Edit Configurations.
2. Delete any JUnit configurations you already have.
3. Click on "Edit configuration templates" and click on "JUnit".
4. Click on "Modify options" and select "Add VM options" if it's not already selected. 
5. In the VM options text input, add the following:
   --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED --add-exports=java.base/sun.security.action=ALL-UNNAMED
6. Click "Apply".
7. In the dropdown that has "Class" selected, change that to "Method" and hit "Apply" again.

You may need to repeat steps 6 and 7. I've found Intellij to be a little finicky with actually applying these changes.

The net effect should be that when you run a JUnit class or method or suite of tests, those VM options are automatically
added to the run configuration that Intellij creates for the class/method/suite. Those VM options are required to give
Spark access to certain JVM modules. They are applied automatically when running the tests via Gradle.

**Alternatively**, you can open Preferences in Intellij and go to 
"Build, Execution, and Deployment" -> "Build Tools" -> "Gradle". Then change "Build and run using" and "Run tests using"
to "Gradle". This should result in Intellij using the `test` configuration in the `marklogic-spark-connector/build.gradle`
file that registers the required JVM options, allowing for tests to run on Java 17.

## Testing text classification

See the `ClassifyAdHocTest` class for instructions on how to test the text classification feature with a 
valid connection to Semaphore.

## Generating code quality reports with SonarQube

Please see our internal Wiki page - search for "Developer Experience SonarQube" - 
for information on setting up SonarQube and using it with this repository.

# Testing with PySpark

The documentation for this project 
[has instructions on using PySpark](https://marklogic.github.io/marklogic-spark-connector/getting-started-pyspark.html) 
with the connector. The documentation instructs a user to obtain the connector from this repository's 
releases page. For development and testing, you will most likely want to build the connector yourself by running the 
following command from the root of this repository:

    ./gradlew clean shadowJar

This will produce a single jar file for the connector in the `./build/libs` directory. 

You can then launch PySpark with the connector available via:

    pyspark --jars marklogic-spark-connector/build/libs/marklogic-spark-connector-2.6-SNAPSHOT.jar

The below command is an example of loading data from the test application deployed via the instructions at the top of 
this page. 

```
df = spark.read.format("marklogic")\
    .option("spark.marklogic.client.uri", "spark-test-user:spark@localhost:8016")\
    .option("spark.marklogic.read.opticQuery", "op.fromView('Medical', 'Authors')")\
    .option("spark.marklogic.read.numPartitions", 8)\
    .load()
```

You now have a Spark dataframe - try some commands out on it:

    df.count()
    df.show(10)
    df.head()

Check out the [PySpark docs](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) for 
more commands you can try out. 

You can query for documents as well - the following shows a simple example along with a technique for converting the
binary content of each document into a string of JSON.

```
import json
from pyspark.sql import functions as F

df = spark.read.format("marklogic")\
    .option("spark.marklogic.client.uri", "spark-test-user:spark@localhost:8016")\
    .option("spark.marklogic.read.documents.collections", "author")\
    .load()
df.show()

df2 = df.select(F.col("content").cast("string"))
df2.head()
json.loads(df2.head()['content'])
```

For a quick test of writing documents, use the following:

```

spark.read.option("header", True).csv("marklogic-spark-connector/src/test/resources/data.csv")\
    .repartition(2)\
    .write.format("marklogic")\
    .option("spark.marklogic.client.uri", "spark-test-user:spark@localhost:8000")\
    .option("spark.marklogic.write.permissions", "spark-user-role,read,spark-user-role,update")\
    .option("spark.marklogic.write.logProgress", 50)\
    .option("spark.marklogic.write.batchSize", 10)\
    .mode("append")\
    .save()
```

# Testing against a local Spark cluster

When you run PySpark, it will create its own Spark cluster. If you'd like to try against a separate Spark cluster
that still runs on your local machine, perform the following steps:

1. Use [sdkman to install Spark](https://sdkman.io/sdks#spark). Run `sdk install spark 3.5.5` since we are currently
building against Spark 3.5.5.
2. `cd ~/.sdkman/candidates/spark/current/sbin`, which is where sdkman will install Spark.
3. Run `./start-master.sh` to start a master Spark node.
4. `cd ../logs` and open the master log file that was created to find the address for the master node. It will be in a
log message similar to `Starting Spark master at spark://NYWHYC3G0W:7077` - copy that address at the end of the message.
5. `cd ../sbin`.
6. Run `./start-worker.sh spark://NYWHYC3G0W:7077`, changing that address as necessary.

You can of course simplify the above steps by adding `SPARK_HOME` to your env and adding `$SPARK_HOME/sbin` to your
path, which thus avoids having to change directories. The log files in `./logs` are useful to tail as well.

The Spark master GUI is at <http://localhost:8080>. You can use this to view details about jobs running in the cluster.

Now that you have a Spark cluster running, you just need to tell PySpark to connect to it:

    pyspark --master spark://NYWHYC3G0W:7077 --jars marklogic-spark-connector/build/libs/marklogic-spark-connector-2.6-SNAPSHOT.jar

You can then run the same commands as shown in the PySpark section above. The Spark master GUI will allow you to 
examine details of each of the commands that you run.

The above approach is ultimately a sanity check to ensure that the connector works properly with a separate cluster
process. 

## Testing spark-submit

Once you have the above Spark cluster running, you can test out 
[spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) which enables submitting a program
and an optional set of jars to a Spark cluster for execution. 

You will need the connector jar available, so run `./gradlew clean shadowJar` if you have not already.

You can then run a test Python program in this repository via the following (again, change the master address as 
needed); note that you run this outside of PySpark, and `spark-submit` is available after having installed PySpark:

    spark-submit --master spark://NYWHYC3G0W:7077 --jars marklogic-spark-connector/build/libs/marklogic-spark-connector-2.6-SNAPSHOT.jar marklogic-spark-connector/src/test/python/test_program.py

You can also test a Java program. To do so, first move the `com.marklogic.spark.TestProgram` class from `src/test/java`
to `src/main/java`. Then run the following:

```
./gradlew clean shadowJar
cd marklogic-spark-connector
spark-submit --master spark://NYWHYC3G0W:7077 --class com.marklogic.spark.TestProgram build/libs/marklogic-spark-connector-2.6-SNAPSHOT.jar
```

Be sure to move `TestProgram` back to `src/test/java` when you are done. 

# Testing the documentation locally

See the section with the same name in the 
[MarkLogic Koop contributing guide](https://github.com/koopjs/koop-provider-marklogic/blob/master/CONTRIBUTING.md).

If you are looking to test the examples in the documentation, please be sure to follow the instructions in the 
"Getting Started" guide. That involves creating an application in MarkLogic that has an app server listening on port
8003. You will use that app server instead of the test-app server on port 8016. 
