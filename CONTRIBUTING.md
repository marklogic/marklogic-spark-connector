To deploy the test application, first create `./gradle-local.properties` and add the following to it:

    mlPassword=the password of your admin user

Then deploy the test application:

    ./gradlew -i mlDeploy

You can then run all the tests:

    ./gradlew test
