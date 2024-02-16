package com.marklogic.spark.reader.file;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Random;

/**
 * Captures the logic from Content Pump for serializing a Jena Triple into a string representation. Note that this does
 * not contain the "escape XML" logic in the MLCP code as we don't care about an XML representation of the triples yet.
 * We just want to return the raw values so they can be added to a Spark row.
 */
class TripleSerializer {

    // These are both used in the MLCP-specific code below for generating a "blank" value.
    private final static long HASH64_STEP = 15485863L;
    private final Random random = new Random();

    public InternalRow serialize(Triple triple) {
        String[] objectValues = serializeObject(triple);
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(serialize(triple.getSubject())),
            UTF8String.fromString(serialize(triple.getPredicate())),
            UTF8String.fromString(objectValues[0]),
            objectValues[1] != null ? UTF8String.fromString(objectValues[1]) : null,
            objectValues[2] != null ? UTF8String.fromString(objectValues[2]) : null,
            null
        });
    }

    private String serialize(Node node) {
        return node.isBlank() ? generateBlankValue(node) : node.toString();
    }

    /**
     * @param triple
     * @return an array containing a string serialization of the object; an optional datatype; and an optional "lang" value.
     */
    private String[] serializeObject(Triple triple) {
        Node node = triple.getObject();
        if (node.isLiteral()) {
            String type = node.getLiteralDatatypeURI();
            String lang = node.getLiteralLanguage();
            if ("".equals(lang)) {
                lang = null;
            }
            if ("".equals(lang) || lang == null) {
                if (type == null) {
                    type = "http://www.w3.org/2001/XMLSchema#string";
                }
            } else {
                type = null;
            }
            return new String[]{node.getLiteralLexicalForm(), type, lang};
        } else if (node.isBlank()) {
            return new String[]{generateBlankValue(node), null, null};
        } else {
            return new String[]{node.toString(), null, null};
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
