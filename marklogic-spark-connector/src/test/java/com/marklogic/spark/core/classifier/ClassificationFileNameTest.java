/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.client.io.Format;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClassificationFileNameTest {

    @ParameterizedTest
    @CsvSource({
        "/dept/report.json, report.json",
        "report.json, report.json",
        "s3a://b/k.pdf, k.pdf",
        "/docs/Report.PDF, Report.PDF",
        "/docs/C#-guide.pdf, C_-guide.pdf",
        "/docs/file?v=1#sec.xml, file_v_1_sec.xml",
        "/docs/my file%20name.pdf, my_file_name.pdf",
        "file:/tmp/my%20report.pdf, my_report.pdf",
        "/docs/100%25done.pdf, 100_done.pdf",
        "/docs/bad%zzescape.pdf, bad_zzescape.pdf",
        "/docs/a+b.pdf, a_b.pdf",
        "/docs/report-.pdf, report.pdf",
        "/docs/report., report",
        "/docs/.env, .env",
        "/path/報告書_2026.pdf, _2026.pdf",
        "/path/報告書.pdf, content.pdf",
        "/dept/report.json/, report.json",
        "C:\\Users\\docs\\guide.docx\\, guide.docx"
    })
    void deriveFileName(String uri, String expectedFileName) {
        assertEquals(expectedFileName, ClassificationFileName.from(uri, Format.BINARY));
    }

    @Test
    void longDirectoryPathWithShortLastSegment() {
        String uri = "file:///data/" + "a/".repeat(150) + "q3-summary.pdf";
        assertEquals("q3-summary.pdf", ClassificationFileName.from(uri, Format.BINARY));
    }

    @Test
    void longLastSegmentWithExtension() {
        String uri = "/a/" + "x".repeat(150) + ".json";
        String result = ClassificationFileName.from(uri, Format.JSON);
        assertEquals(ClassificationFileName.MAX_FILE_NAME_LENGTH, result.length());
        assertEquals("x".repeat(95) + ".json", result);
    }

    @Test
    void longLastSegmentWithoutExtension() {
        String uri = "/a/" + "y".repeat(150);
        String result = ClassificationFileName.from(uri, Format.UNKNOWN);
        assertEquals(ClassificationFileName.MAX_FILE_NAME_LENGTH, result.length());
        assertEquals("y".repeat(100), result);
    }

    @Test
    void multibyteLongSegmentProducesBoundedAsciiBytes() {
        String uri = "/path/" + "報告書a".repeat(60) + ".pdf";
        String result = ClassificationFileName.from(uri, Format.BINARY);
        byte[] utf8Bytes = result.getBytes(StandardCharsets.UTF_8);
        assertEquals(result.length(), utf8Bytes.length, "Sanitized file name must be pure ASCII so char length == byte length");
        assertEquals(ClassificationFileName.MAX_FILE_NAME_LENGTH, utf8Bytes.length);
        assertTrue(result.endsWith(".pdf"));
    }

    @Test
    void quotesAndControlCharactersAreSanitized() {
        String uri = "/docs/bad\"name\r\nhere.xml";
        assertEquals("bad_name_here.xml", ClassificationFileName.from(uri, Format.XML));
    }

    @Test
    void fakeLongExtensionIsTreatedAsStem() {
        String uri = "/notes/v1.2.3-" + "a".repeat(120);
        String result = ClassificationFileName.from(uri, Format.TEXT);
        assertEquals(ClassificationFileName.MAX_FILE_NAME_LENGTH, result.length());
        assertTrue(result.startsWith("v1.2.3-aaa"));
        assertTrue(result.endsWith(".txt"));
    }

    @ParameterizedTest
    @CsvSource(nullValues = "NULL", value = {
        "/docs/12345, JSON, 12345.json",
        "/docs/12345, XML, 12345.xml",
        "/docs/12345, TEXT, 12345.txt",
        "/docs/12345, BINARY, 12345",
        "/docs/12345, NULL, 12345",
        "/docs/.env, TEXT, .env.txt"
    })
    void formatExtensionAddedWhenUriHasNone(String uri, Format format, String expected) {
        assertEquals(expected, ClassificationFileName.from(uri, format));
    }

    @Test
    void truncationDoesNotLeaveTrailingPunctuationBeforeExtension() {
        String uri = "/a/" + "x".repeat(95) + "-" + "y".repeat(20) + ".pdf";
        assertEquals("x".repeat(95) + ".pdf", ClassificationFileName.from(uri, Format.BINARY));
    }

    @Test
    void truncationThatRemovesAllAlphanumericsFallsBackToContent() {
        String uri = "/a/" + "-".repeat(150) + "a.pdf";
        assertEquals("content.pdf", ClassificationFileName.from(uri, Format.BINARY));
    }

    @ParameterizedTest
    @CsvSource(nullValues = "NULL", value = {
        "/, JSON, content.json",
        "///, XML, content.xml",
        "'', TEXT, content.txt",
        "NULL, BINARY, content.bin",
        "/, UNKNOWN, content.bin",
        "/, NULL, content.bin",
        "/???, JSON, content.json",
        "/..., XML, content.xml"
    })
    void fallbackWhenNoUsableSegment(String uri, Format format, String expected) {
        assertEquals(expected, ClassificationFileName.from(uri, format));
    }
}
