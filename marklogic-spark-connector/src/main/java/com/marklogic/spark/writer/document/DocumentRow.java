/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Encapsulates a row that confirms to {@code DocumentRowSchema} and may have additional columns as well. Intended to
 * handle converting from row-specific types into data structures preferred by {@code DocBuilder.DocumentInputs}.
 */
class DocumentRow {

    private final InternalRow row;

    DocumentRow(InternalRow row) {
        this.row = row;
    }

    BytesHandle getContent(Format documentFormat) {
        BytesHandle content = new BytesHandle(row.getBinary(1));
        // Ensures a format is set if possible, before the content is used in any other operations, including writing
        // the content to MarkLogic.
        setHandleFormat(content, documentFormat);
        return content;
    }

    String getFormat() {
        return row.isNullAt(2) ? null : row.getString(2);
    }

    DocumentMetadataHandle getMetadata() {
        return DocumentRowSchema.makeDocumentMetadata(row);
    }

    private void setHandleFormat(BytesHandle bytesHandle, Format documentFormat) {
        if (documentFormat != null) {
            bytesHandle.withFormat(documentFormat);
        } else {
            String format = getFormat();
            if (format != null) {
                try {
                    bytesHandle.withFormat(Format.valueOf(format.toUpperCase()));
                } catch (IllegalArgumentException e) {
                    // We don't ever expect this to happen, but in case it does - we'll proceed with a null format
                    // on the handle, as it's not essential that it be set.
                    if (Util.MAIN_LOGGER.isDebugEnabled()) {
                        Util.MAIN_LOGGER.debug("Unable to set format on row with URI: {}; format: {}; error: {}",
                            row.getString(0), format, e.getMessage());
                    }
                }
            }
        }
    }
}
