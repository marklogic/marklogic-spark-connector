/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Captures the logic from Content Pump for serializing a Jena Triple or Quad into a string representation.
 */
class RdfSerializer {

    private static final String DEFAULT_GRAPH = "http://marklogic.com/semantics#default-graph";

    InternalRow serialize(Triple triple) {
        String[] objectValues = serializeObject(triple.getObject());
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(serialize(triple.getSubject())),
            UTF8String.fromString(serialize(triple.getPredicate())),
            UTF8String.fromString(objectValues[0]),
            objectValues[1] != null ? UTF8String.fromString(objectValues[1]) : null,
            objectValues[2] != null ? UTF8String.fromString(objectValues[2]) : null,
            null
        });
    }

    InternalRow serialize(Quad quad) {
        String[] objectValues = serializeObject(quad.getObject());
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(serialize(quad.getSubject())),
            UTF8String.fromString(serialize(quad.getPredicate())),
            UTF8String.fromString(objectValues[0]),
            objectValues[1] != null ? UTF8String.fromString(objectValues[1]) : null,
            objectValues[2] != null ? UTF8String.fromString(objectValues[2]) : null,
            quad.getGraph() != null ?
                UTF8String.fromString(serialize(quad.getGraph())) :
                UTF8String.fromString(DEFAULT_GRAPH)
        });
    }

    private String serialize(Node node) {
        return node.isBlank() ? generateBlankValue(node) : node.toString();
    }

    /**
     * @param object
     * @return an array containing a string serialization of the object; an optional datatype; and an optional "lang" value.
     */
    private String[] serializeObject(Node object) {
        if (object.isLiteral()) {
            String type = object.getLiteralDatatypeURI();
            String lang = object.getLiteralLanguage();
            if ("".equals(lang)) {
                lang = null;
            }
            if (lang != null && !lang.trim().isEmpty()) {
                // MarkLogic uses this datatype when a string has a lang associated with it.
                type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
            } else if ("".equals(lang) || lang == null) {
                if (type == null) {
                    type = "http://www.w3.org/2001/XMLSchema#string";
                }
            } else {
                type = null;
            }
            return new String[]{object.getLiteralLexicalForm(), type, lang};
        } else if (object.isBlank()) {
            return new String[]{generateBlankValue(object), null, null};
        } else {
            return new String[]{object.toString(), null, null};
        }
    }

    /**
     * Fixed in 2.6.0 to use Jena's native blank node ID generation.
     * See https://en.wikipedia.org/wiki/Blank_node for more details on blank nodes.
     *
     * @return
     */
    private String generateBlankValue(Node blankNode) {
        return String.format("http://marklogic.com/semantics/blank/%s", blankNode);
    }
}
