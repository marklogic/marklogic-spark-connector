---
layout: default
title: Java
parent: Getting Started
nav_order: 4
---

The MarkLogic connector is published to [Maven Central](https://central.sonatype.com/namespace/com.marklogic) and 
can thus be expressed as a regular dependency of a Java application that also depends on the Spark APIs. 

As an example, please see the project configuration in the 
[java-dependency example project](https://github.com/marklogic/marklogic-spark-connector/blob/master/examples/java-dependency)
for how to depend on the MarkLogic connector as a library. The `org.example.App` class in the project demonstrates
a very simple Spark Java program for accessing the data in the application deployed via the [Setup guide](setup.md).
