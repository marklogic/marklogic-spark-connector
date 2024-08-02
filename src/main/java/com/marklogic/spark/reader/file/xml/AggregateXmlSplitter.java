/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file.xml;

import com.marklogic.client.datamovement.XMLSplitter;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.file.FileContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Knows how to split an aggregate XML document and return a row for each user-defined child element. Each row has
 * a schema matching that of {@code DocumentRowSchema}.
 */
class AggregateXmlSplitter {

    private final Iterator<StringHandle> contentStream;
    private final String identifierForErrors;

    // Optional, both can be null.
    private final String uriElement;
    private final String uriNamespace;

    private int rowCounter = 1;

    private static XMLInputFactory xmlInputFactory;

    static {
        xmlInputFactory = XMLInputFactory.newFactory();
        // The following prevents XXE attacks, per Sonar java:S2755 rule.
        // Note that setting XMLConstants.ACCESS_EXTERNAL_DTD and XMLConstants.ACCESS_EXTERNAL_SCHEMA to empty
        // strings is also suggested by the Sonar S2755 docs and will work fine in this connector project - but it
        // will result in warnings in the Flux application that oddly cause no data to be read. So do not set those
        // to empty strings here. The below config satisfies Sonar in terms of preventing XXE attacks and does not
        // impact functionality.
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    }

    /**
     * @param identifierForErrors allows the caller of this class to provide a useful description to be included in
     *                            any errors to help users with debugging.
     * @param inputStream         the stream of aggregate XML data
     * @param fileContext
     */
    AggregateXmlSplitter(String identifierForErrors, InputStream inputStream, FileContext fileContext) {
        this.identifierForErrors = identifierForErrors;
        this.uriElement = fileContext.getStringOption(Options.READ_AGGREGATES_XML_URI_ELEMENT);
        this.uriNamespace = fileContext.getStringOption(Options.READ_AGGREGATES_XML_URI_NAMESPACE);
        final String namespace = fileContext.getStringOption(Options.READ_AGGREGATES_XML_NAMESPACE);
        final String element = fileContext.getStringOption(Options.READ_AGGREGATES_XML_ELEMENT);
        final String encoding = fileContext.getStringOption(Options.READ_FILES_ENCODING);

        final XMLSplitter<StringHandle> splitter = this.uriElement != null ?
            new XMLSplitter<>(new UriElementExtractingVisitor(namespace, element, uriNamespace, uriElement)) :
            XMLSplitter.makeSplitter(namespace, element);

        try {
            XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(inputStream, encoding);
            this.contentStream = splitter.split(reader).iterator();
        } catch (IOException | XMLStreamException e) {
            throw new ConnectorException(
                String.format("Unable to read XML at %s; cause: %s", this.identifierForErrors, e.getMessage()), e
            );
        }
    }

    boolean hasNext() {
        try {
            return this.contentStream.hasNext();
        } catch (Exception e) {
            String message = String.format("Unable to read XML from %s; cause: %s", identifierForErrors, e.getMessage());
            throw new ConnectorException(message, e);
        }
    }

    /**
     * @param uriPrefix used to construct a URI if no uriElement was specified
     * @return a row corresponding to the {@code DocumentRowSchema}
     */
    InternalRow nextRow(String uriPrefix) {
        StringHandle stringHandle;
        try {
            stringHandle = this.contentStream.next();
        } catch (RuntimeException ex) {
            String message = String.format("Unable to read XML from %s; cause: %s",
                this.identifierForErrors, ex.getMessage());
            throw new ConnectorException(message, ex);
        }

        final String initialUri = determineInitialUri(stringHandle, uriPrefix);
        rowCounter++;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(initialUri),
            ByteArray.concat(stringHandle.get().getBytes()),
            UTF8String.fromString("xml"),
            null, null, null, null, null
        });
    }

    private String determineInitialUri(StringHandle stringHandle, String uriPrefix) {
        if (stringHandle instanceof StringHandleWithUriValue) {
            String uriValue = ((StringHandleWithUriValue) stringHandle).getUriValue();
            if (uriValue == null) {
                String message = String.format("No occurrence of URI element '%s' found in aggregate element %d in %s",
                    this.uriElement, rowCounter, this.identifierForErrors);
                throw new ConnectorException(message);
            }
            return uriValue;
        }
        return String.format("%s-%d.xml", uriPrefix, rowCounter);
    }
}
