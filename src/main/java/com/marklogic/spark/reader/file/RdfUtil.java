package com.marklogic.spark.reader.file;

public interface RdfUtil {

    static boolean isQuadsFile(String filename) {
        return isTrigFile(filename) ||
            filename.endsWith(".nq") ||
            filename.endsWith(".nq.gz");
    }

    static boolean isTrigFile(String filename) {
        return filename.endsWith(".trig") || filename.endsWith(".trig.gz");
    }
}
