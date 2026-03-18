/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import java.util.Set;

/**
 * Encapsulates the logic for constructing and parsing metadata entry names in archive files.
 * Metadata entries encode the document format in their names to support proper handling during
 * import, especially for documents that were transformed to binary.
 * <p>
 * Format: {@code <documentUri>.<format>.metadata}
 * <p>
 * Example: {@code /doc3.xml.BINARY.metadata}
 */
public record MetadataEntryName(String documentUri, String format) {

    private static final String SUFFIX = ".metadata";
    private static final Set<String> KNOWN_FORMATS = Set.of("JSON", "XML", "TEXT", "BINARY");

    /**
     * Parses a metadata entry name to extract document URI and format, validating against the actual document entry name.
     * This helps distinguish between pre-3.1.0 archives (e.g., "/author/1.json" + "/author/1.json.metadata")
     * and 3.1.0+ archives (e.g., "/author/1.json" + "/author/1.json.JSON.metadata").
     *
     * @param metadataEntryName the metadata entry name (e.g., "/doc3.xml.BINARY.metadata")
     * @param documentEntryName the actual document entry name (e.g., "/doc3.xml")
     * @return a MetadataEntryName instance with parsed components
     */
    public static MetadataEntryName parse(String metadataEntryName, String documentEntryName) {
        // Highly unexpected, but no reason to fail if either is true.
        if (documentEntryName == null || !metadataEntryName.endsWith(SUFFIX)) {
            return new MetadataEntryName(metadataEntryName, null);
        }

        // Check if this is pre-3.1.0 format: documentEntryName + ".metadata"
        if (metadataEntryName.equals(documentEntryName + SUFFIX)) {
            return new MetadataEntryName(documentEntryName, null);
        }

        // Check if this matches the 3.1.0+ format: documentEntryName + "." + FORMAT + ".metadata"
        for (String knownFormat : KNOWN_FORMATS) {
            String expectedMetadataName = documentEntryName + "." + knownFormat + SUFFIX;
            if (metadataEntryName.equalsIgnoreCase(expectedMetadataName)) {
                return new MetadataEntryName(documentEntryName, knownFormat);
            }
        }

        // Unexpected pattern - shouldn't happen, but default to no format
        return new MetadataEntryName(documentEntryName, null);
    }

    /**
     * Builds the full metadata entry name.
     *
     * @return the metadata entry name (e.g., "/doc3.xml.BINARY.metadata")
     */
    public String makeMetadataEntryName() {
        if (format == null || format.isEmpty()) {
            return documentUri + SUFFIX;
        }
        return documentUri + "." + format + SUFFIX;
    }

    @Override
    public String toString() {
        return makeMetadataEntryName();
    }
}
