package com.marklogic.spark.reader.file;

import com.marklogic.client.datamovement.XMLSplitter;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
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

        try {
            XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(inputStream, encoding);
            this.contentStream = XMLSplitter.makeSplitter(namespace, element).split(reader).iterator();
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
     * @param pathPrefix used to construct a path if no uriElement was specified
     * @return a row corresponding to the {@code DocumentRowSchema}
     */
    InternalRow nextRow(String pathPrefix) {
        String xml;
        try {
            xml = this.contentStream.next().get();
        } catch (RuntimeException ex) {
            String message = String.format("Unable to read XML from %s; cause: %s",
                this.identifierForErrors, ex.getMessage());
            throw new ConnectorException(message, ex);
        }

        final String path = this.uriElement != null && !this.uriElement.trim().isEmpty() ?
            extractUriElementValue(xml) :
            pathPrefix + "-" + rowCounter + ".xml";

        rowCounter++;

        byte[] content = xml.getBytes();
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(path),
            ByteArray.concat(content),
            UTF8String.fromString("xml"),
            null, null, null, null, null
        });
    }

    /**
     * MLCP has undocumented support for attribute references via "@(attribute-name)". We are not supporting this yet
     * as we are using XMLSplitter to find the user-defined element, and XMLSplitter does not support finding
     * attributes. Additionally, this feature is still fairly limited in comparison to the "URI template" that the
     * connector supports. Ultimately, we'd want to support N path expressions against both Spark columns and against
     * a JSON or XML tree in a single Spark column.
     *
     * @param xml
     * @return
     */
    private String extractUriElementValue(String xml) {
        Iterator<StringHandle> iterator;
        XMLSplitter<StringHandle> splitter = XMLSplitter.makeSplitter(this.uriNamespace, this.uriElement);
        splitter.setVisitor(new UriElementVisitor(this.uriNamespace, this.uriElement));
        try {
            iterator = splitter.split(new ByteArrayInputStream(xml.getBytes())).iterator();
        } catch (Exception e) {
            // We don't expect this to ever occur, as if the XML couldn't be parsed, an error would have been thrown
            // when the child element was originally extracted. But still have to catch an exception.
            String message = String.format("Unable to parse XML in aggregate element %d in %s; cause: %s",
                rowCounter, this.identifierForErrors, e.getMessage());
            throw new ConnectorException(message, e);
        }

        if (!iterator.hasNext()) {
            String message = String.format("No occurrence of URI element '%s' found in aggregate element %d in %s",
                this.uriElement, rowCounter, this.identifierForErrors);
            throw new ConnectorException(message);
        }
        return iterator.next().get();
    }

    /**
     * Extends the Java Client visitor class so that it can return a handle containing the text of the
     * user-defined URI element.
     */
    private class UriElementVisitor extends XMLSplitter.BasicElementVisitor {
        public UriElementVisitor(String nsUri, String localName) {
            super(nsUri, localName);
        }

        @Override
        public StringHandle makeBufferedHandle(XMLStreamReader xmlStreamReader) {
            String text;
            try {
                text = xmlStreamReader.getElementText();
            } catch (XMLStreamException e) {
                String message = String.format(
                    "Unable to get text from URI element '%s' found in aggregate element %d in %s; cause: %s",
                    uriElement, rowCounter, identifierForErrors, e.getMessage()
                );
                throw new ConnectorException(message, e);
            }
            return new StringHandle(text);
        }
    }
}
