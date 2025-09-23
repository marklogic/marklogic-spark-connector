/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.springframework.test.util.AssertionErrors.fail;

public interface SerializeUtil {

    /**
     * Used by tests that verify that objects that need to be serializable are in fact serializable.
     */
    static Object serialize(Object o) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            o = ois.readObject();
            ois.close();
            return o;
        } catch (Exception e) {
            fail("Could not serialize object: " + e.getMessage());
            return null;
        }
    }
}
