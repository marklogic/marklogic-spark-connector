/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownParamTest extends AbstractPushDownTest {
    static final String LEXICON_BIND_QUERY = wrapNonViewQuery(
    "op.fromLexicons("
    + "{diagnosis: cts.elementReference('diagnosis')})"
    + ".where(fn.contains($c.diagnosis, op.param('searchTerm')))");

    static final String SPARQL_BIND_QUERY = wrapNonViewQuery(
    "op.fromSPARQL(\""
    + "SELECT * FROM <http://example.org/graph> "
    + "WHERE {?s ?p ?o FILTER (?s = IRI(@subject))}"
    + "\")");

    /**
     * This test demonstrates a parameter binding on a lexicon.
     * Lexicons are useful for filtering with specific collation or other advanced equality comparisons.
     */
    @Test
    void equalToWithOpticParam() {
        assertEquals(1, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, LEXICON_BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "searchTerm", "default_value")
            .load()
            .filter("searchTerm == 'heat-stroke'")
            .collectAsList()
            .size(), "Verifying that an optic binding can be parameterized");
    }

    /**
     * This test demonstrates two common use cases:
     * - How to parameterize a SQL or SPARQL query, where a filter outside may not meet design requirements
     * - How to cast a filter value type when you may not have a suitable type avilable downstream
     */
    @Test
    void equalToWithSparqlParam() {
        assertEquals(8, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, SPARQL_BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "subject", "default_value")
            .load()
            .filter("subject == 'http://vocabulary.worldbank.org/taxonomy/451'")
            .collectAsList()
            .size(), "Verifying that a SPARQL binding can be parameterized");
    }

    /**
     * This test makes sure that the default value is used when a matching filter is not pushed down.
     */
    @Test
    void equalToWithDefaultParam() {
        assertEquals(8, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, SPARQL_BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "subject", "http://vocabulary.worldbank.org/taxonomy/451")
            .load()
            .collectAsList()
            .size(), "Verifying that a default parameter is applied");
    }

    /**
     * This test makes sure that a filter on a different column won't overwrite the default binding.
     */
    @Test
    void equalToWithDefaultParamPlusFilter() {
        assertEquals(1, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, SPARQL_BIND_QUERY)
            .option(Options.READ_COLUMN_PARAMS_PREFIX + "subject", "http://vocabulary.worldbank.org/taxonomy/451")
            .load()
            .filter("o == 0")
            .collectAsList()
            .size(), "Verifying that a default parameter isn't overwritten by another filter");
    }

    /**
     * Queries have to start with fromView.  This is a workaround for tests that don't need a view.
     * @param query
     * @return
     */
    private static String wrapNonViewQuery(String query) {
        return QUERY_WITH_NO_QUALIFIER + ".limit(1).select('rowid').joinCrossProduct(" + query + ")";
    }
}