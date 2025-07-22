/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.marklogic.spark.JsonRowSerializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.catalyst.json.JacksonParser;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.Function2;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    public JsonRowDeserializer(StructType schema) {
        this.jacksonParser = newJacksonParser(schema);
        this.jsonParserCreator = CreateJacksonParser::string;
        this.utf8StringCreator = UTF8String::fromString;
    }

    public InternalRow deserializeJson(String json) {
        return this.jacksonParser.parse(json, this.jsonParserCreator, this.utf8StringCreator).head();
    }

    private JacksonParser newJacksonParser(StructType schema) {
        final boolean allowArraysAsStructs = true;
        List<Filter> filters = new ArrayList<>();
        scala.collection.immutable.Seq<Filter> filterSeq = scala.collection.immutable.List.from(CollectionConverters.asScala(filters)).toSeq();
        JSONOptions jsonOptions = new JsonRowSerializer(schema, new HashMap<>()).getJsonOptions();
        return new JacksonParser(schema, jsonOptions, allowArraysAsStructs, filterSeq);
    }
}
