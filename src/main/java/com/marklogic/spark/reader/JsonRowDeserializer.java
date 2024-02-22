package com.marklogic.spark.reader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.json.JacksonParser;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.Function2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.compat.java8.JFunction;

import java.util.ArrayList;

/**
 * Handles deserializing a JSON object into a Spark InternalRow. This is accomplished via Spark's JacksonParser.
 * This class is critical to our connector, though unfortunately there doesn't seem to be much in the way of public
 * docs for it. Source code for it can be found at:
 * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/json/JacksonParser.scala
 * <p>
 * As noted in the code, JacksonParser can translate a string of JSON into a Spark InternalRow based on a schema.
 * That's exactly what we want, so we don't need to have any knowledge of how to convert to Spark's set of data
 * types.
 */
public class JsonRowDeserializer {

    private final JacksonParser jacksonParser;
    private final Function2<JsonFactory, String, JsonParser> jsonParserCreator;
    private final Function1<String, UTF8String> utf8StringCreator;

    // Ignoring warnings about JFunction.func until an alternative can be found.
    @SuppressWarnings("java:S1874")
    public JsonRowDeserializer(StructType schema) {
        this.jacksonParser = newJacksonParser(schema);

        // Used https://github.com/scala/scala-java8-compat in the DHF Spark 2 connector. Per the README for
        // scala-java8-compat, we should be able to use scala.jdk.FunctionConverters since those are part of Scala
        // 2.13. However, that is not yet working within PySpark. So sticking with this "legacy" approach as it seems
        // to work fine in both vanilla Spark (i.e. JUnit tests) and PySpark.
        this.jsonParserCreator = JFunction.func(CreateJacksonParser::string);
        this.utf8StringCreator = JFunction.func(UTF8String::fromString);
    }

    public InternalRow deserializeJson(String json) {
        return this.jacksonParser.parse(json, this.jsonParserCreator, this.utf8StringCreator).head();
    }

    private JacksonParser newJacksonParser(StructType schema) {
        final boolean allowArraysAsStructs = true;
        final Seq<Filter> filters = JavaConverters.asScalaIterator(new ArrayList<Filter>().iterator()).toSeq();
        return new JacksonParser(schema, Util.DEFAULT_JSON_OPTIONS, allowArraysAsStructs, filters);
    }
}
