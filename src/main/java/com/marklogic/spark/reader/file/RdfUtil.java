package com.marklogic.spark.reader.file;

public interface RdfUtil {

    static boolean isQuadsFile(String filename) {
        return filename.endsWith(".trig") ||
            filename.endsWith(".trig.gz") ||
            filename.endsWith(".nq") ||
            filename.endsWith(".nq.gz");
    }
}
