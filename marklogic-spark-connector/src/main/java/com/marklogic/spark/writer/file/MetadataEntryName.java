/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import java.util.Locale;
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
     * Parses a metadata entry name to extract document URI and format.
     *
     * @param metadataEntryName the metadata entry name (e.g., "/doc3.xml.BINARY.metadata")
     * @return a MetadataEntryName instance with parsed components
     */
    public static MetadataEntryName parse(String metadataEntryName) {
        if (!metadataEntryName.endsWith(SUFFIX)) {
            return new MetadataEntryName(metadataEntryName, null);
        }

        final String withoutSuffix = metadataEntryName.substring(0, metadataEntryName.length() - SUFFIX.length());

        // Check if this is the new format by looking for a known format suffix
        for (String knownFormat : KNOWN_FORMATS) {
            String formatSuffix = "." + knownFormat;
            if (withoutSuffix.toUpperCase(Locale.ROOT).endsWith(formatSuffix)) {
                String documentUri = withoutSuffix.substring(0, withoutSuffix.length() - formatSuffix.length());
                return new MetadataEntryName(documentUri, knownFormat);
            }
        }

        // Pre-3.1.0 format: /doc.xml.metadata - no format encoded
        return new MetadataEntryName(withoutSuffix, null);
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
