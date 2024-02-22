# This is intended solely for testing spark-submit as described in the CONTRIBUTING.md file.

from pyspark.sql import SparkSession

SparkSession.builder.getOrCreate().read.format("marklogic") \
  .option("spark.marklogic.client.uri", "spark-test-user:spark@localhost:8016") \
  .option("spark.marklogic.read.opticQuery", "op.fromView('Medical', 'Authors', '')") \
  .load() \
  .show()
