package com.marklogic.spark.reader.document;

import com.marklogic.spark.ContextSupport;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DocumentContext extends ContextSupport {

    DocumentContext(CaseInsensitiveStringMap options) {
        super(options.asCaseSensitiveMap());
    }

}
