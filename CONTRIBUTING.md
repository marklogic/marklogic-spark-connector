This is an evolving guide for developers interested in developing and testing this project. This guide assumes that you
have cloned this repository to your local workstation. 

# Running the tests

To deploy the test application, first create `./gradle-local.properties` and add the following to it:

    mlPassword=the password of your admin user

Then deploy the test application:

    ./gradlew -i mlDeploy

You can then run all the tests:

    ./gradlew test

# Running the tests against a 3-node cluster

This project includes a `docker-compose.yaml` file for standing up a 3-node MarkLogic cluster. This is useful for 
manually testing the connector's support for distributing requests to multiple hosts in a cluster. The automated 
tests can also be run against this cluster too.

To run the automated tests, first perform the following steps to set up a 3-node cluster and a Docker image 
containing all of this project's Gradle dependencies:

1. [Install Docker](https://docs.docker.com/get-docker/).
2. Ensure that you don't have a MarkLogic instance running locally.
3. Run `./gradlew dockerUp` (Gradle tasks are included as shortcuts for running Docker commands). This will start up 
   the 3-node cluster, with one host listening on the standard ports from 8000 to 8002.
4. Run `./gradlew -i mlDeploy` to deploy this project's test app to the 3-node cluster.
5. Run `./gradlew dockerBuildCache` as a mostly one-time step to build a Docker image that contains all of this 
   project's Gradle dependencies. This will allow the next step to run much more quickly. You'll only need to run 
   this again when the project's Gradle dependencies change.

You're now ready to run the automated tests; be sure you've created a `gradle-local.properties` file with 
`mlPassword` in it per the instructions in the section above this. The tests can be run via:

    ./gradlew dockerTest

All the tests should pass successfully. In addition, you should be able to inspect the `8016_AccessLog.txt` file on each
MarkLogic host and see that each host received requests while the tests were being run. 


# Testing with PySpark

First, [follow the instructions](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) on 
installing PySpark. You'll need to install Python 3 first. [pyenv](https://github.com/pyenv/pyenv#installation) is 
recommended for doing so, as it simplifies installing multiple versions of Python and easily switching between them. 

Once you've installed PySpark, run it to make sure all is well:

    pyspark

That should open up a Python shell and print some logging like this:

```
Using Python version 3.9.11 (main, Sep 27 2022 13:33:29)
Spark context Web UI available at http://10.114.228.34:4040
Spark context available as 'sc' (master = local[*], app id = local-1682019905427).
SparkSession available as 'spark'.
```

Quit out of the Python shell by hitting `ctrl-D`. 

Build the MarkLogic Spark connector:

    ./gradlew clean shadowJar

This will produce a single jar file for the connector in the `./build/libs` directory. 

Next, from any directory, run the following, assuming that you cloned this project to `/Users/myusername`:

    pyspark --jars /Users/myusername/marklogic-spark-connector/build/libs/marklogic-spark-connector-1.0-SNAPSHOT-all.jar

The above command will start another Python shell, and the MarkLogic Spark connector will be available to Spark. 

Now, let's make use of the connector - paste the following into the Python terminal, altering the connection details
and the Optic query as necessary (this defaults to a query that will work against this project's test application, 
which can be deployed via the instructions above for running this project's tests):

```
df = spark.read.format("com.marklogic.spark")\
    .option("spark.marklogic.client.host", "localhost")\
    .option("spark.marklogic.client.port", "8016")\
    .option("spark.marklogic.client.username", "admin")\
    .option("spark.marklogic.client.password", "admin")\
    .option("spark.marklogic.client.authType", "digest")\
    .option("spark.marklogic.read.opticDsl", "op.fromView('Medical', 'Authors')")\
    .load()
```

You now have a Spark dataframe - try some commands out on it:

    df.count()
    df.show(10)
    df.head()

Check out the [PySpark docs](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) for 
more commands you can try out. 
