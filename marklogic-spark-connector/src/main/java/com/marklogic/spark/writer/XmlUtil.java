/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;

import java.io.StringReader;

public abstract class XmlUtil {

    private static SAXBuilder saxBuilder;

    static {
        saxBuilder = new SAXBuilder();
    }

    public static Document buildDocument(String xml) {
        try {
            return saxBuilder.build(new StringReader(xml));
        } catch (Exception ex) {
            throw new ConnectorException(String.format("Unable to read XML; cause: %s", ex.getMessage()), ex);
        }
    }

    public static Document extractDocument(AbstractWriteHandle handle) {
        if (handle instanceof JDOMHandle) {
            return ((JDOMHandle) handle).get();
        }
        String xml = HandleAccessor.contentAsString(handle);
        return buildDocument(xml);
    }

    private XmlUtil() {
    }
}
