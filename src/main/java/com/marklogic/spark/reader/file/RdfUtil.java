package com.marklogic.spark.reader.file;

import org.apache.jena.riot.Lang;

public interface RdfUtil {

    static boolean isQuadsFile(String filename) {
        return isTrigFile(filename) || isTrixFile(filename) ||
            filename.endsWith(".nq") ||
            filename.endsWith(".nq.gz");
    }

    static Lang getQuadsLang(String filename) {
        if (isTrigFile(filename)) {
            return Lang.TRIG;
        } else if (isTrixFile(filename)) {
            return Lang.TRIX;
        }
        return Lang.NQ;
    }

    private static boolean isTrigFile(String filename) {
        return filename.endsWith(".trig") || filename.endsWith(".trig.gz");
    }

    private static boolean isTrixFile(String filename) {
        return filename.endsWith(".trix") || filename.endsWith(".trix.gz");
    }
}
