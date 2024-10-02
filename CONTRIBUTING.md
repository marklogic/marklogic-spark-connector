This guide covers how to develop and test this project. It assumes that you have cloned this repository to your local
workstation.

You must use Java 11 or higher for developing, testing, and building this project. If you wish to use Sonar as 
described below, you must use Java 17 or higher.

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

Alternatively, if you would like to test against a 3-node MarkLogic cluster with a load balancer in front of it, 
run `docker compose -f docker-compose-3nodes.yaml up -d --build`.

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

## Generating code quality reports with SonarQube

In order to use SonarQube, you must have used Docker to run this project's `docker-compose.yml` file, and you must
have the services in that file running and you must use Java 17 to run the Gradle `sonar` task.

To configure the SonarQube service, perform the following steps:

1. Go to http://localhost:9000 .
2. Login as admin/admin. SonarQube will ask you to change this password; you can choose whatever you want ("password" works).
3. Click on "Create project manually".
4. Enter "marklogic-spark" for the Project Name; use that as the Project Key too.
5. Enter "develop" as the main branch name.
6. Click on "Next".
7. Click on "Use the global setting" and then "Create project".
8. On the "Analysis Method" page, click on "Locally".
9. In the "Provide a token" panel, click on "Generate". Copy the token.
10. Add `systemProp.sonar.token=your token pasted here` to `gradle-local.properties` in the root of your project, creating
that file if it does not exist yet.

To run SonarQube, run the following Gradle tasks using Java 17, which will run all the tests with code coverage and 
then generate a quality report with SonarQube:

    ./gradlew test sonar

If you do not add `systemProp.sonar.token` to your `gradle-local.properties` file, you can specify the token via the
following:

    ./gradlew test sonar -Dsonar.token=paste your token here

When that completes, you will see a line like this near the end of the logging:

    ANALYSIS SUCCESSFUL, you can find the results at: http://localhost:9000/dashboard?id=marklogic-spark

Click on that link. If it's the first time you've run the report, you'll see all issues. If you've run the report
before, then SonarQube will show "New Code" by default. That's handy, as you can use that to quickly see any issues
you've introduced on the feature branch you're working on. You can then click on "Overall Code" to see all issues.

Note that if you only need results on code smells and vulnerabilities, you can repeatedly run `./gradlew sonar`
without having to re-run the tests.

You can also force Gradle to run `sonar` if any tests fail:

    ./gradlew clean test sonar --continue

## Accessing MarkLogic logs in Grafana

This project's `docker-compose-3nodes.yaml` file includes
[Grafana, Loki, and promtail services](https://grafana.com/docs/loki/latest/clients/promtail/) for the primary reason of
collecting MarkLogic log files and allowing them to be viewed and searched via Grafana.

Once you have run `docker compose`, you can access Grafana at http://localhost:3000 . Follow these instructions to
access MarkLogic logging data:

1. Click on the hamburger in the upper left hand corner and select "Explore", or simply go to
   http://localhost:3000/explore .
2. Verify that "Loki" is the default data source - you should see it selected in the upper left hand corner below
   the "Home" link.
3. Click on the "Select label" dropdown and choose `job`. Click on the "Select value" label for this filter and
   select `marklogic` as the value.
4. Click on the blue "Run query" button in the upper right hand corner.

You should now see logs from all 3 nodes in the MarkLogic cluster.

# Testing with PySpark

The documentation for this project 
[has instructions on using PySpark](https://marklogic.github.io/marklogic-spark-connector/getting-started-pyspark.html) 
with the connector. The documentation instructs a user to obtain the connector from this repository's 
releases page. For development and testing, you will most likely want to build the connector yourself by running the 
following command from the root of this repository:

    ./gradlew clean shadowJar

This will produce a single jar file for the connector in the `./build/libs` directory. 

You can then launch PySpark with the connector available via:

    pyspark --jars build/libs/marklogic-spark-connector-2.4-SNAPSHOT.jar

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

spark.read.option("header", True).csv("src/test/resources/data.csv")\
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

1. Use [sdkman to install Spark](https://sdkman.io/sdks#spark). Run `sdk install spark 3.4.3` since we are currently
building against Spark 3.4.3.
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

    pyspark --master spark://NYWHYC3G0W:7077 --jars build/libs/marklogic-spark-connector-2.4-SNAPSHOT.jar

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

    spark-submit --master spark://NYWHYC3G0W:7077 --jars build/libs/marklogic-spark-connector-2.4-SNAPSHOT.jar src/test/python/test_program.py

You can also test a Java program. To do so, first move the `com.marklogic.spark.TestProgram` class from `src/test/java`
to `src/main/java`. Then run `./gradlew clean shadowJar` to rebuild the connector jar. Then run the following:

    spark-submit --master spark://NYWHYC3G0W:7077 --class com.marklogic.spark.TestProgram build/libs/marklogic-spark-connector-2.4-SNAPSHOT.jar

Be sure to move `TestProgram` back to `src/test/java` when you are done. 

# Testing the documentation locally

See the section with the same name in the 
[MarkLogic Koop contributing guide](https://github.com/koopjs/koop-provider-marklogic/blob/master/CONTRIBUTING.md).

If you are looking to test the examples in the documentation, please be sure to follow the instructions in the 
"Getting Started" guide. That involves creating an application in MarkLogic that has an app server listening on port
8003. You will use that app server instead of the test-app server on port 8016. 
