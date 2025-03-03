/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import java.util.HashMap;
import java.util.Map;

public abstract class ExtractionUtil {

    /**
     * Defines XML namespaces associated with metadata extracted by Apache Tika.
     */
    private static final Map<String, String> PREFIX_TO_NAMESPACE_MAPPING = new HashMap<>();

    static {
        PREFIX_TO_NAMESPACE_MAPPING.put("dc", "http://purl.org/dc/elements/1.1/");
        PREFIX_TO_NAMESPACE_MAPPING.put("dcterms", "http://purl.org/dc/terms/");
        PREFIX_TO_NAMESPACE_MAPPING.put("exif", "http://ns.adobe.com/exif/1.0/");
        PREFIX_TO_NAMESPACE_MAPPING.put("pdf", "http://ns.adobe.com/pdf/1.3/");
        PREFIX_TO_NAMESPACE_MAPPING.put("tiff", "http://ns.adobe.com/tiff/1.0/");
        PREFIX_TO_NAMESPACE_MAPPING.put("xhtml", "http://www.w3.org/1999/xhtml");
        PREFIX_TO_NAMESPACE_MAPPING.put("xmp", "http://ns.adobe.com/xap/1.0/");
    }

    public static String getNamespace(String prefix) {
        return PREFIX_TO_NAMESPACE_MAPPING.get(prefix);
    }
    
    private ExtractionUtil() {
    }
}
