This guide covers how to develop and test this project. It assumes that you have cloned this repository to your local
workstation.

Due to the use of the Sonar plugin for Gradle, you must use Java 11 or higher for developing and testing the project. 
The `build.gradle` file for this project ensures that the connector is built to run on Java 8 or higher. 

To begin, you need to deploy the test application in this project to MarkLogic. You can do so either on your own 
installation of MarkLogic, or you can use `docker-compose` to install MarkLogic, optionally as a 3-node cluster with 
a load balancer in front of it.

## Installing MarkLogic with docker-compose

If you wish to use `docker-compose`, perform the following steps before deploying the test application.

1. [Install Docker](https://docs.docker.com/get-docker/).
2. Ensure that you don't have a MarkLogic instance running locally (if you do, you may run into port conflicts in 
   the next step).
3. Run `docker-compose up -d --build`.

The above will result in a new MarkLogic instance with a single node. 

Alternatively, if you would like to test against a 3-node MarkLogic cluster with a load balancer in front of it, 
run `docker-compose -f docker-compose-3nodes.yaml up -d --build`.

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


## Generating code quality reports with SonarQube

In order to use SonarQube, you must have used Docker to run this project's `docker-compose.yml` file and you must
have the services in that file running.

To configure the SonarQube service, perform the following steps:

1. Go to http://localhost:9000 .
2. Login as admin/admin. SonarQube will ask you to change this password; you can choose whatever you want ("password" works).
3. Click on "Create project manually".
4. Enter "marklogic-spark" for the Project Name; use that as the Project Key too.
5. Enter "develop" as the main branch name.
6. Click on "Next".
7. Click on "Use the global setting" and then "Create project".
8. On the "Analysis Method" page, click on "Locally".
9. In the "Provide a token" panel, click on "Generate". Copy the token to a safe place (you can always generate a new one).

One place to paste your Sonar token would be in `gradle-local.properties`, as that file is gitignore'd. You'll still 
need to paste it in the command shown next though.

To run SonarQube, run the following Gradle tasks, which will run all of the tests with code coverage and then generate
a quality report with SonarQube:

    ./gradlew test sonar -Dsonar.token=paste your token here

When that completes, you will see a line like this near the end of the logging:

    ANALYSIS SUCCESSFUL, you can find the results at: http://localhost:9000/dashboard?id=marklogic-spark

Click on that link. If it's the first time you've run the report, you'll see all issues. If you've run the report
before, then SonarQube will show "New Code" by default. That's handy, as you can use that to quickly see any issues
you've introduced on the feature branch you're working on. You can then click on "Overall Code" to see all issues.

Note that if you only need results on code smells and vulnerabilities, you can repeatedly run `./gradlew sonar -Dsonar:token`
without having to re-run the tests.

# Testing with PySpark

The documentation for this project 
[has instructions on using PySpark](https://marklogic.github.io/marklogic-spark-connector/getting-started-pyspark.html) 
with the connector. The documentation instructs a user to obtain the connector from this repository's 
releases page. For development and testing, you will most likely want to build the connector yourself by running the 
following command from the root of this repository:

    ./gradlew clean shadowJar

This will produce a single jar file for the connector in the `./build/libs` directory. 

You can then launch PySpark with the connector available via:

    pyspark --jars build/libs/marklogic-spark-connector-2.2-SNAPSHOT.jar

The below command is an example of loading data from the test application deployed via the instructions at the top of 
this page. 

```
df = spark.read.format("com.marklogic.spark")\
    .option("spark.marklogic.client.host", "localhost")\
    .option("spark.marklogic.client.port", "8016")\
    .option("spark.marklogic.client.username", "admin")\
    .option("spark.marklogic.client.password", "admin")\
    .option("spark.marklogic.client.authType", "digest")\
    .option("spark.marklogic.read.opticQuery", "op.fromView('Medical', 'Authors')")\
    .load()
```

You now have a Spark dataframe - try some commands out on it:

    df.count()
    df.show(10)
    df.head()

Check out the [PySpark docs](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) for 
more commands you can try out. 

# Testing the documentation locally

See the section with the same name in the 
[MarkLogic Koop contributing guide](https://github.com/koopjs/koop-provider-marklogic/blob/master/CONTRIBUTING.md).

If you are looking to test the examples in the documentation, please be sure to follow the instructions in the 
"Getting Started" guide. That involves creating an application in MarkLogic that has an app server listening on port
8003. You will use that app server instead of the test-app server on port 8016. 
