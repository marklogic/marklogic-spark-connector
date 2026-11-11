/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.client.io.Format;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Derives a short, safe file name from a document URI for whole-document classification requests to Semaphore.
 * Semaphore's {@code TypeGuesser} uses the file name (primarily its extension) to detect the document format, and
 * fails when the full source URI is too long.
 */
final class ClassificationFileName {

    static final int MAX_FILE_NAME_LENGTH = 100;
    static final int MAX_EXTENSION_LENGTH = 10;

    private static final Pattern EXTENSION = Pattern.compile("[A-Za-z0-9]{1," + MAX_EXTENSION_LENGTH + "}");
    // Matches runs of characters outside [A-Za-z0-9.-] (including '_') so unsafe chars are replaced with '_'
    // and contiguous runs of '_' are collapsed in a single pass.
    private static final Pattern UNSAFE_OR_UNDERSCORES = Pattern.compile("[^A-Za-z0-9.-]+");
    private static final Pattern ALPHANUMERIC = Pattern.compile("[A-Za-z0-9]");
    private static final Pattern TRAILING_PUNCTUATION = Pattern.compile("[._-]+$");

    private ClassificationFileName() {
    }

    static String from(String uri, Format format) {
        String segment = percentDecode(lastSegment(uri));
        if (segment.isEmpty()) {
            return "content" + defaultExtension(format);
        }
        String stem = segment;
        String ext = "";
        int dot = segment.lastIndexOf('.');
        if (dot > 0 && EXTENSION.matcher(segment.substring(dot + 1)).matches()) {
            stem = segment.substring(0, dot);
            ext = segment.substring(dot);
        }
        // Without an extension, give Semaphore's TypeGuesser a hint when the format is known.
        if (ext.isEmpty()) {
            ext = knownFormatExtension(format);
        }
        stem = UNSAFE_OR_UNDERSCORES.matcher(stem).replaceAll("_");
        int maxStem = MAX_FILE_NAME_LENGTH - ext.length();
        if (stem.length() > maxStem) {
            stem = stem.substring(0, maxStem);
        }
        // Avoid names like "abc-.pdf" or "abc..pdf", e.g. after truncation.
        stem = TRAILING_PUNCTUATION.matcher(stem).replaceAll("");
        if (!ALPHANUMERIC.matcher(stem).find()) {
            stem = "content";
            if (ext.isEmpty()) {
                ext = defaultExtension(format);
            }
        }
        return stem + ext;
    }

    /**
     * Spark reports file paths as URIs, so e.g. a space in a file name arrives as "%20". Decodes valid %XX sequences
     * as UTF-8; '+' is left as-is. Returns the input unchanged if it contains a malformed escape.
     */
    private static String percentDecode(String segment) {
        if (segment.indexOf('%') < 0) {
            return segment;
        }
        try {
            return URLDecoder.decode(segment.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return segment;
        }
    }

    private static String lastSegment(String uri) {
        if (uri == null || uri.isEmpty()) {
            return "";
        }
        int end = uri.length();
        while (end > 0 && isSlash(uri.charAt(end - 1))) {
            end--;
        }
        if (end == 0) {
            return "";
        }
        int lastSlash = -1;
        for (int i = end - 1; i >= 0; i--) {
            if (isSlash(uri.charAt(i))) {
                lastSlash = i;
                break;
            }
        }
        return uri.substring(lastSlash + 1, end);
    }

    private static boolean isSlash(char c) {
        return c == '/' || c == '\\';
    }

    private static String defaultExtension(Format format) {
        String ext = knownFormatExtension(format);
        return ext.isEmpty() ? ".bin" : ext;
    }

    private static String knownFormatExtension(Format format) {
        if (format == Format.JSON) {
            return ".json";
        }
        if (format == Format.XML) {
            return ".xml";
        }
        if (format == Format.TEXT) {
            return ".txt";
        }
        return "";
    }
}
