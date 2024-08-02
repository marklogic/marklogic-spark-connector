/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file.xml;

import com.marklogic.client.datamovement.XMLSplitter;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import javax.xml.stream.XMLStreamReader;

/**
 * Supports extracting a URI element value for each aggregate element.
 */
class UriElementExtractingVisitor extends XMLSplitter.BasicElementVisitor {

    private final String uriNamespace;
    private final String uriElement;

    UriElementExtractingVisitor(String nsUri, String localName, String uriNamespace, String uriElement) {
        super(nsUri, localName);
        this.uriNamespace = uriNamespace;
        this.uriElement = uriElement;
    }

    @Override
    public StringHandle makeBufferedHandle(XMLStreamReader xmlStreamReader) {
        UriElementExtractingReader reader = new UriElementExtractingReader(xmlStreamReader, uriNamespace, uriElement);
        String content = serialize(reader);
        String uriValue = reader.getUriValue();
        return new StringHandleWithUriValue(content, uriValue).withFormat(Format.XML);
    }
}
