/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

public class ConnectorException extends RuntimeException {

    public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

}
