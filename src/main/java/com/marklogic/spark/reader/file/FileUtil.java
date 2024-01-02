package com.marklogic.spark.reader.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public interface FileUtil {

    static byte[] readBytes(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[1024];
        int offset = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            baos.write(buffer, offset, read);
            offset += read;
        }
        return baos.toByteArray();
    }
}
