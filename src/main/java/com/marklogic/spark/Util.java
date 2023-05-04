package com.marklogic.spark;

import org.apache.spark.sql.catalyst.json.JSONOptions;
import scala.collection.immutable.HashMap;

public abstract class Util {

    public final static JSONOptions DEFAULT_JSON_OPTIONS = new JSONOptions(
        new HashMap<>(),

        // As verified via tests, this default timezone ID is overridden by a user via the spark.sql.session.timeZone option.
        "Z",

        // We don't expect corrupted records - i.e. corrupted values - to be present in the index. But Spark
        // requires this to be set. See
        // https://medium.com/@sasidharan-r/how-to-handle-corrupt-or-bad-record-in-apache-spark-custom-logic-pyspark-aws-430ddec9bb41
        // for more information.
        "_corrupt_record"
    );

}
