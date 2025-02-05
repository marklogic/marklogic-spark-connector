/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

public class OtherMain {

    public static void main(String[] args) throws Exception {
        Object doc = Class.forName("dev.langchain4j.data.document.Document").getDeclaredConstructor(String.class).newInstance("Hi");
        System.out.println("Here's a Document: " + doc);
    }
}
