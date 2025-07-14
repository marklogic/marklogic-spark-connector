/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileCopyUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateCopyrightsTest {

    @Disabled("Intended to be run manually when needed.")
    @Test
    void test() throws Exception {
        final String oldCopyright = "2024 MarkLogic Corporation. All Rights Reserved.";
        final String newCopyright = "2025 MarkLogic Corporation. All Rights Reserved.";

        // To produce output.txt, run the following in the root of the repository.
        // git log --name-only --after="2024-12-31T23:59:59-00:00" --oneline > output.txt
        assertTrue(new File("../output.txt").exists());
        try (BufferedReader reader = new BufferedReader(new FileReader("../output.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.endsWith(".java")) {
                    final String filePath = "../" + line;
                    if (new File(filePath).exists()) {
                        String text = FileCopyUtils.copyToString(new FileReader(filePath));
                        if (text.contains(oldCopyright)) {
                            text = text.replaceAll(oldCopyright, newCopyright);
                            System.out.println("Updating: " + filePath);
                            FileCopyUtils.copy(text, new FileWriter(filePath));
                        }
                    }
                }
                line = reader.readLine();
            }
        }
    }
}
