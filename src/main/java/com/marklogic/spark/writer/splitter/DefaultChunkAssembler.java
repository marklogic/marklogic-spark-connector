/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.spark.writer.XmlUtil;
import dev.langchain4j.data.segment.TextSegment;
import org.jdom2.Document;
import org.jdom2.Element;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Default impl that only supports XML and adding chunks to the source document.
 * Will expand this via future stories.
 */
public class DefaultChunkAssembler implements ChunkAssembler {

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        Document doc;
        if (sourceDocument.getContent() instanceof JDOMHandle) {
            doc = ((JDOMHandle) sourceDocument.getContent()).get();
        } else {
            String content = HandleAccessor.contentAsString(sourceDocument.getContent());
            doc = XmlUtil.buildDocument(content);
        }

        final Element chunks = new Element("chunks");
        doc.getRootElement().addContent(chunks);
        textSegments.forEach(textSegment -> chunks
            .addContent(new Element("chunk").addContent(new Element("text").addContent(textSegment.text())))
        );

        return Stream.of((DocumentWriteOperation) new DocumentWriteOperationImpl(
            sourceDocument.getUri(),
            sourceDocument.getMetadata(),
            new JDOMHandle(doc)
        )).iterator();
    }
}
