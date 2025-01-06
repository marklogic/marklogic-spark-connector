---
layout: default
title: Jupyter
parent: Getting Started
nav_order: 3
---

[Project Jupyter](https://jupyter.org/) provides a set of tools for working with notebooks, code, and data. The 
MarkLogic connector can be easily integrated into these tools to allow users to access and analyze data in 
MarkLogic. 

Before going further, be sure you've followed the instructions in the [setup guide](setup.md) for
obtaining the connector and deploying an example application to MarkLogic.

## Install Jupyter

To get started, install either [JupyterLab or Jupyter Notebook](https://jupyter.org/install). Both of these tools
allow you to work with the connector in the same fashion. The rest of this guide will assume the use of Jupyter 
Notebook, though the instructions will work for JupyterLab as well.

Note that if you have not already installed PySpark, run `pip install pyspark` using the same Python interpreter that 
you will use to run Jupyter Notebook or JupyterLab.

Once you have installed, started, and accessed Jupyter Notebook in your web browser - in a default Notebook 
installation, you should be able to access it at http://localhost:8889/tree - click on "New" in the upper right hand 
corner of the Notebook interface and select "Python 3 (ipykernel)" to create a new notebook.

## Using the connector

In the first cell in the notebook created above, enter the following to allow Jupyter Notebook to access the MarkLogic 
connector and also to initialize Spark:

```
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars "/path/to/marklogic-spark-connector-2.5.1.jar" pyspark-shell'

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName('My Notebook').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark
```

The path of `/path/to/marklogic-spark-connector-2.5.1.jar` should be changed to match the location of the connector 
jar on your filesystem. You are free to customize the `spark` variable in any manner you see fit as well. 

Now that you have an initialized Spark session, you can run any of the examples found in the 
[guide for using PySpark](pyspark.md).

## Example notebook

The [getting-started example project](https://github.com/marklogic/marklogic-spark-connector/tree/master/examples/getting-started)
includes an 
[example notebook](https://github.com/marklogic/marklogic-spark-connector/tree/master/examples/getting-started/marklogic-spark-getting-started.ipynb)
that you can open in Jupyter Notebook. This allows you to try a few examples with a working notebook without having to 
enter anything in. 
