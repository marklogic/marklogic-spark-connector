This is an evolving guide for developers interested in developing and testing this project. This guide assumes that you
have cloned this repository to your local workstation. 

# Do this first!

In order to develop and/or test the MarkLogic Spark connector, or to try out the PySpark instructions below, you first 
need to deploy the test application in this project to MarkLogic. You can do so either on your own installation of 
MarkLogic, or you can use `docker-compose` to install a 3-node MarkLogic cluster with a load balancer in front of it. 

## Installing a 3-node cluster with docker-compose

If you wish to use `docker-compose`, perform the following steps before deploying the test application.

1. [Install Docker](https://docs.docker.com/get-docker/).
2. Ensure that you don't have a MarkLogic instance running locally (if you do, you may run into port conflicts in 
   the next step).
3. Run `./gradlew dockerUp` (Gradle tasks are included as shortcuts for running Docker commands). This will start up
   a 3-node cluster with a load balancer in front of it. Additionally, the 8000/8001/8002 ports are available on the 
   "bootstrap" node of the cluster for accessing the out-of-the-box MarkLogic applications.

### Accessing MarkLogic logs in Grafana

This project's `docker-compose.yaml` file includes 
[Grafana, Loki, and promtail services](https://grafana.com/docs/loki/latest/clients/promtail/) for the primary reason of 
collecting MarkLogic log files and allowing them to be viewed and searched via Grafana. 

Once you have run `docker-compose`, you can access Grafana at http://localhost:3000 . Follow these instructions to 
access MarkLogic logging data:

1. Click on the hamburger in the upper left hand corner and select "Explore", or simply go to 
   http://localhost:3000/explore . 
2. Verify that "Loki" is the default data source - you should see it selected in the upper left hand corner below 
   the "Home" link.
3. Click on the "Select label" dropdown and choose `job`. Click on the "Select value" label for this filter and 
   select `marklogic` as the value.
4. Click on the blue "Run query" button in the upper right hand corner.

You should now see logs from all 3 nodes in the MarkLogic cluster. 


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

If you installed MarkLogic using this project's `docker-compose.yaml` file, you can also run the tests from within the 
Docker environment by first running the following task:

    ./gradlew dockerBuildCache

The above task is a mostly one-time step to build a Docker image that contains all of this project's Gradle 
dependencies. This will allow the next step to run much more quickly. You'll only need to run this again when the 
project's Gradle dependencies change.

You can then run the tests from within the Docker environment via the following task:

    ./gradlew dockerTest


# Testing with PySpark

First, [follow the instructions](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) for 
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
