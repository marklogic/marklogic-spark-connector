/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j;

public class MarkLogicLangchainException extends RuntimeException {

    public MarkLogicLangchainException(String message) {
        super(message);
    }

    public MarkLogicLangchainException(String message, Throwable cause) {
        super(message, cause);
    }

}
