/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file.xml;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

/**
 * Knows how to extract a URI element value while the XML for an aggregate element is being read and serialized.
 */
class UriElementExtractingReader extends StreamReaderDelegate {

    private XMLStreamReader source;
    private final String uriNamespace;
    private final String uriElement;

    // Used to track when the URI element is detected.
    private boolean isReadingUriElement;
    private String uriValue;

    UriElementExtractingReader(XMLStreamReader source, String uriNamespace, String uriElement) {
        super(source);
        this.source = source;
        this.uriNamespace = uriNamespace;
        this.uriElement = uriElement;
    }

    @Override
    public int next() throws XMLStreamException {
        int value = source.next();
        if (value == XMLStreamConstants.START_ELEMENT) {
            // Only use the first instance of the URI element that is found.
            if (matchesUriElement() && this.uriValue == null) {
                this.isReadingUriElement = true;
                this.uriValue = "";
            }
        } else if (value == XMLStreamConstants.CHARACTERS) {
            if (this.isReadingUriElement) {
                this.uriValue += source.getText();
            }
        } else if (value == XMLStreamConstants.END_ELEMENT && this.isReadingUriElement && matchesUriElement()) {
            this.isReadingUriElement = false;
        }
        return value;
    }

    private boolean matchesUriElement() {
        return source.getLocalName().equals(uriElement) &&
            (this.uriNamespace == null || this.uriNamespace.equals(source.getNamespaceURI()));
    }

    String getUriValue() {
        return uriValue;
    }
}
