package com.marklogic.spark.writer.rdf;

import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.spark.writer.DocBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.util.UUID;

/**
 * Keeps track of a sem:triples document containing 1 to many sem:triple elements.
 */
public class TriplesDocument {

    static final Namespace SEMANTICS_NAMESPACE = Namespace.getNamespace("sem", "http://marklogic.com/semantics");
    private static final Namespace XML_NAMESPACE = Namespace.getNamespace("xml", "http://www.w3.org/XML/1998/namespace");

    // May allow the user to configure this.
    private static final int TRIPLES_PER_DOCUMENT = 100;

    private final String graph;
    private final Document document;
    private int tripleCount;

    public TriplesDocument(String graph) {
        this.graph = graph;
        this.document = new Document(new Element("triples", SEMANTICS_NAMESPACE));
    }

    public TriplesDocument(String graph, String origin) {
        this.graph = graph;
        this.document = new Document(new Element("triples", SEMANTICS_NAMESPACE));
        Element originElement = new Element("origin", SEMANTICS_NAMESPACE);
        originElement.addContent(origin);
        document.getRootElement().addContent(originElement);
    }

    public void addTriple(InternalRow row) {
        Element triple = new Element("triple", SEMANTICS_NAMESPACE);
        document.getRootElement().addContent(triple);
        triple.addContent(new Element("subject", SEMANTICS_NAMESPACE).addContent(row.getString(0)));
        triple.addContent(new Element("predicate", SEMANTICS_NAMESPACE).addContent(row.getString(1)));
        Element object = new Element("object", SEMANTICS_NAMESPACE).addContent(row.getString(2));
        if (!row.isNullAt(3)) {
            object.setAttribute("datatype", row.getString(3));
        }
        if (!row.isNullAt(4)) {
            object.setAttribute("lang", row.getString(4), XML_NAMESPACE);
        }
        triple.addContent(object);
        tripleCount++;
    }

    public boolean hasMaxTriples() {
        return tripleCount >= TRIPLES_PER_DOCUMENT;
    }

    public DocBuilder.DocumentInputs buildDocument() {
        JDOMHandle content = new JDOMHandle(document);
        String uri = String.format("/triplestore/%s.xml", UUID.randomUUID());
        return new DocBuilder.DocumentInputs(uri, content, null, null, graph);
    }

    public Object[] toTriplesDocumentRow() {
        Object[] row = new Object[3];
        row[0] = UTF8String.fromString(String.format("/triplestore/%s.xml", UUID.randomUUID()));
        row[1] = UTF8String.fromString(new XMLOutputter(Format.getCompactFormat()).outputString(document));
        row[2] = UTF8String.fromString(graph);
        return row;
    }
}
