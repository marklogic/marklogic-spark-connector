package com.marklogic.spark.reader.file;

import com.marklogic.client.datamovement.XMLSplitter;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

class AggregateXMLFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(AggregateXMLFileReader.class);

    private final Path path;
    private final InputStream inputStream;
    private final Iterator<StringHandle> contentStream;

    // Optional, both can be null.
    private final String uriElement;
    private final String uriNamespace;

    private int rowCounter = 1;

    AggregateXMLFileReader(FilePartition partition, Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        if (logger.isTraceEnabled()) {
            logger.trace("Reading path: {}", partition.getPath());
        }
        this.path = new Path(partition.getPath());
        this.uriElement = properties.get(Options.READ_AGGREGATES_XML_URI_ELEMENT);
        this.uriNamespace = properties.get(Options.READ_AGGREGATES_XML_URI_NAMESPACE);

        String namespace = properties.get(Options.READ_AGGREGATES_XML_NAMESPACE);
        String element = properties.get(Options.READ_AGGREGATES_XML_ELEMENT);
        try {
            // Contrary to writing files, testing has shown no difference in performance with using e.g. FileInputStream
            // instead of fileSystem.open when fileSystem is a LocalFileSystem.
            this.inputStream = path.getFileSystem(hadoopConfiguration.value()).open(path);
            this.contentStream = XMLSplitter.makeSplitter(namespace, element).split(this.inputStream).iterator();
        } catch (Exception e) {
            // Interestingly, this won't fail if the file is malformed or not XML. It's only when we try to get the
            // first element.
            throw new ConnectorException(String.format("Unable to read file %s", this.path), e);
        }
    }

    @Override
    public boolean next() {
        try {
            return this.contentStream.hasNext();
        } catch (RuntimeException e) {
            String message = String.format("Unable to read XML from %s; cause: %s", this.path, e.getMessage());
            throw new ConnectorException(message, e);
        }
    }

    @Override
    public InternalRow get() {
        String xml = this.contentStream.next().get();
        String uri = this.uriElement != null && !this.uriElement.trim().isEmpty() ?
            extractUriElementValue(xml) :
            this.path.toString() + "-" + rowCounter + ".xml";

        rowCounter++;

        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), null, 0l,
            ByteArray.concat(xml.getBytes()),
        });
    }

    @Override
    public void close() {
        if (this.inputStream != null) {
            IOUtils.closeQuietly(this.inputStream);
        }
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
            String message = String.format("Unable to parse XML in aggregate element %d in file %s; cause: %s", rowCounter, this.path, e.getMessage());
            throw new ConnectorException(message, e);
        }

        if (!iterator.hasNext()) {
            String message = String.format("No occurrence of URI element '%s' found in aggregate element %d in file %s",
                this.uriElement, rowCounter, this.path);
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
                String message = String.format("Unable to get text from URI element '%s' found in aggregate element %d in file %s: cause: %s",
                    uriElement, rowCounter, path, e.getMessage());
                throw new ConnectorException(message, e);
            }
            return new StringHandle(text);
        }
    }
}
