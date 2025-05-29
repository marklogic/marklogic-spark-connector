/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadUrisViaSecondaryQueryTest extends AbstractIntegrationTest {

    private static final String JAVASCRIPT_QUERY = """
        var URIs;
        const citationIds = cts.elementValues(xs.QName("CitationID"), null, null, cts.documentQuery(URIs));
        cts.uris(null, null, cts.andQuery([
            cts.notQuery(cts.documentQuery(URIs)),
            cts.collectionQuery('author'),
            cts.jsonPropertyValueQuery('CitationID', citationIds)
        ]))
        """;

    private static final String XQUERY_QUERY = """
        declare namespace json = "http://marklogic.com/xdmp/json";
        declare variable $URIs external;
        let $values := json:array-values($URIs)
        let $citationIds := cts:element-values(xs:QName("CitationID"), (), (), cts:document-query($values))
        return cts:uris((), (), cts:and-query((
            cts:not-query(cts:document-query($values)),
            cts:collection-query('author'),
            cts:json-property-value-query('CitationID', $citationIds)
        )))""";

    @Test
    void javascript() {
        verifySecondaryQuery(Options.READ_SECONDARY_URIS_JAVASCRIPT, JAVASCRIPT_QUERY);
    }

    @Test
    void xquery() {
        verifySecondaryQuery(Options.READ_SECONDARY_URIS_XQUERY, XQUERY_QUERY);
    }

    @Test
    void invoke() {
        verifySecondaryQuery(Options.READ_SECONDARY_URIS_INVOKE, "/findCustomerUrisViaCitationId.xqy");
    }

    @Test
    void javascriptFile(@TempDir Path tempDir) throws Exception {
        File tempFile = new File(tempDir.toFile(), "findAuthorUrisViaCitationId.js");
        FileCopyUtils.copy(JAVASCRIPT_QUERY.getBytes(StandardCharsets.UTF_8), tempFile);

        verifySecondaryQuery(Options.READ_SECONDARY_URIS_JAVASCRIPT_FILE, tempFile.getAbsolutePath());
    }

    @Test
    void xqueryFile(@TempDir Path tempDir) throws Exception {
        File tempFile = new File(tempDir.toFile(), "findAuthorUrisViaCitationId.xqy");
        FileCopyUtils.copy(XQUERY_QUERY.getBytes(StandardCharsets.UTF_8), tempFile);

        verifySecondaryQuery(Options.READ_SECONDARY_URIS_XQUERY_FILE, tempFile.getAbsolutePath());
    }

    @Test
    void javascriptWithVars() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json")
            .option(Options.READ_SECONDARY_URIS_JAVASCRIPT, "Sequence.from([URI1, URI2])")
            .option(Options.READ_SECONDARY_URIS_VARS_PREFIX + "URI1", "/author/author2.json")
            .option(Options.READ_SECONDARY_URIS_VARS_PREFIX + "URI2", "/author/author3.json")
            .load()
            .select("uri")
            .collectAsList();

        List<String> uris = rows.stream().map(row -> row.getString(0)).toList();
        assertEquals(3, uris.size());
        Stream.of("/author/author1.json", "/author/author2.json", "/author/author3.json")
            .forEach(uri -> assertTrue(uris.contains(uri), "Expecting to find URI: " + uri));
    }

    private void verifySecondaryQuery(String optionName, String optionValue) {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, """
                /author/author1.json
                /author/author2.json""")
            .option(optionName, optionValue)
            .load()
            .select("uri")
            .collectAsList();

        assertEquals(5, rows.size(), "Expecting 5 URIs - the 2 original URIs and 3 URIs via the secondary query. " +
            "The 3 additional URIs come from the query on the CitationID property, where CitationID must be the " +
            "value of CitationID found in the original URIs.");

        List<String> uris = rows.stream().map(row -> row.getString(0)).toList();
        Stream.of("/author/author1.json", "/author/author2.json",
                "/author/author4.json", "/author/author8.json", "/author/author14.json")
            .forEach(uri -> assertTrue(uris.contains(uri), "Expecting to find URI: " + uri));
    }
}
