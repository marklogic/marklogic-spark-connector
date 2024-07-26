package com.marklogic.spark.reader.file;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Random;

/**
 * Captures the logic from Content Pump for serializing a Jena Triple or Quad into a string representation.
 */
class RdfSerializer {

    private static final String DEFAULT_GRAPH = "http://marklogic.com/semantics#default-graph";

    // These are both used in the MLCP-specific code below for generating a "blank" value.
    private static final long HASH64_STEP = 15485863L;

    // Sonar is suspicious about the use of Random, the actual random number doesn't matter for any functionality.
    @SuppressWarnings("java:S2245")
    private final Random random = new Random();

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
     * @param triple
     * @return an array containing a string serialization of the object; an optional datatype; and an optional "lang" value.
     */
    private String[] serializeObject(Node object) {
        if (object.isLiteral()) {
            String type = object.getLiteralDatatypeURI();
            String lang = object.getLiteralLanguage();
            if ("".equals(lang)) {
                lang = null;
            }
            if (lang != null && lang.trim().length() > 0) {
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
     * Reuses copy/pasted code from the MLCP codebase for generating a blank value for a "blank node" - see
     * https://en.wikipedia.org/wiki/Blank_node for more details. It is not known why a UUID isn't used.
     *
     * @return
     */
    private String generateBlankValue(Node blankNode) {
        String value = Long.toHexString(
            hash64(
                fuse(scramble(System.currentTimeMillis()), random.nextLong()),
                blankNode.getBlankNodeLabel()
            )
        );
        return "http://marklogic.com/semantics/blank/" + value;
    }

    private long hash64(long value, String str) {
        char[] arr = str.toCharArray();
        for (int i = 0; i < str.length(); i++) {
            value = (value + Character.getNumericValue(arr[i])) * HASH64_STEP;
        }
        return value;
    }

    private long fuse(long a, long b) {
        return rotl(a, 8) ^ b;
    }

    private long scramble(long x) {
        return x ^ rotl(x, 20) ^ rotl(x, 40);
    }

    private long rotl(long x, long y) {
        return (x << y) ^ (x >> (64 - y));
    }
}
