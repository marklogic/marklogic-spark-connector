/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
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

    private XMLStreamReader reader;
    private final String uriNamespace;
    private final String uriElement;

    // Used to track when the URI element is detected.
    private boolean isReadingUriElement;
    private String uriValue;

    UriElementExtractingReader(XMLStreamReader reader, String uriNamespace, String uriElement) {
        super(reader);
        this.reader = reader;
        this.uriNamespace = uriNamespace;
        this.uriElement = uriElement;
    }

    @Override
    public int next() throws XMLStreamException {
        int value = super.next();
        if (value == XMLStreamConstants.START_ELEMENT) {
            // Only use the first instance of the URI element that is found.
            if (matchesUriElement() && this.uriValue == null) {
                this.isReadingUriElement = true;
                this.uriValue = "";
            }
        } else if (value == XMLStreamConstants.CHARACTERS) {
            if (this.isReadingUriElement) {
                this.uriValue += reader.getText();
            }
        } else if (value == XMLStreamConstants.END_ELEMENT && this.isReadingUriElement && matchesUriElement()) {
            this.isReadingUriElement = false;
        }
        return value;
    }

    private boolean matchesUriElement() {
        return reader.getLocalName().equals(uriElement) &&
            (this.uriNamespace == null || this.uriNamespace.equals(reader.getNamespaceURI()));
    }

    String getUriValue() {
        return uriValue;
    }
}
