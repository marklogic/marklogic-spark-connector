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
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

/**
 * Encapsulates a row that confirms to {@code DocumentRowSchema} and may have additional columns as well.
 */
class DocumentRow {

    public final static String CLASSIFED_TEXT_COLUMN_NAME = "classificationResponse";
    private final InternalRow row;
    private final StructType schema;

    DocumentRow(InternalRow row, StructType schema) {
        this.row = row;
        this.schema = schema;
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

    Optional<String> getExtractedText() {
        int index = getOptionalFieldIndex(schema, "extractedText");
        return index > -1 ? Optional.of(row.getString(index)) : Optional.empty();
    }

    Optional<byte[]> getClassificationResponse() {
        int index = getOptionalFieldIndex(schema, CLASSIFED_TEXT_COLUMN_NAME);
        return index > -1 ? Optional.of(row.getBinary(index)) : Optional.empty();
    }

    DocumentMetadataHandle getMetadata() {
        return DocumentRowSchema.makeDocumentMetadata(row);
    }

    private int getOptionalFieldIndex(StructType schema, String fieldName) {
        // We know what the first set of fields should be, so check each field after the expected set of document
        // row fields.
        for (int i = DocumentRowSchema.SCHEMA.size(); i < schema.size(); i++) {
            if (fieldName.equals(schema.fields()[i].name())) {
                return i;
            }
        }
        return -1;
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
