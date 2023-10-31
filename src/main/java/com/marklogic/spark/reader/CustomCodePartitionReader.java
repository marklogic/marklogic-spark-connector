package com.marklogic.spark.reader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.CustomCodeContext;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.json.CreateJacksonParser;
import org.apache.spark.sql.catalyst.json.JacksonParser;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.Function2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.compat.java8.JFunction;

import java.util.ArrayList;

class CustomCodePartitionReader implements PartitionReader {

    private final ServerEvaluationCall serverEvaluationCall;
    private final boolean isCustomSchema;

    private EvalResultIterator evalResultIterator;

    private final JacksonParser jacksonParser;
    private final Function2<JsonFactory, String, JsonParser> jsonParserCreator;
    private final Function1<String, UTF8String> utf8StringCreator;


    public CustomCodePartitionReader(CustomCodeContext customCodeContext) {
        this.serverEvaluationCall = customCodeContext.buildCall(
            Options.READ_INVOKE, Options.READ_JAVASCRIPT, Options.READ_XQUERY
        );
        this.isCustomSchema = customCodeContext.isCustomSchema();

        // TODO Big pile of duplication here. Will refactor this in a follow-up PR.
        this.jacksonParser = newJacksonParser(customCodeContext.getSchema());
        this.jsonParserCreator = JFunction.func((jsonFactory, someString) -> CreateJacksonParser.string(jsonFactory, someString));
        this.utf8StringCreator = JFunction.func((someString) -> UTF8String.fromString(someString));
    }

    private JacksonParser newJacksonParser(StructType schema) {
        // TODO More duplication here to refactor in a follow-up PR.
        final boolean allowArraysAsStructs = true;
        final Seq<Filter> filters = JavaConverters.asScalaIterator(new ArrayList().iterator()).toSeq();
        return new JacksonParser(schema, Util.DEFAULT_JSON_OPTIONS, allowArraysAsStructs, filters);
    }

    @Override
    public boolean next() {
        if (this.evalResultIterator == null) {
            this.evalResultIterator = this.serverEvaluationCall.eval();
        }
        return this.evalResultIterator.hasNext();
    }

    @Override
    public InternalRow get() {
        String val = this.evalResultIterator.next().getString();
        if (this.isCustomSchema) {
            return this.jacksonParser.parse(val, this.jsonParserCreator, this.utf8StringCreator).head();
        }
        return new GenericInternalRow(new Object[]{UTF8String.fromString(val)});
    }

    @Override
    public void close() {
        this.evalResultIterator.close();
    }
}
