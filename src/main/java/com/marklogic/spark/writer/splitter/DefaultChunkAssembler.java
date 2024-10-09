/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
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

    private final boolean sourceDocumentsAreText;

    public DefaultChunkAssembler(boolean sourceDocumentsAreText) {
        this.sourceDocumentsAreText = sourceDocumentsAreText;
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        if (sourceDocumentsAreText) {
            return writeChunksToSeparateDocument(sourceDocument, textSegments);
        }

        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());
        addChunksToDocument(doc, textSegments);

        return Stream.of((DocumentWriteOperation) new DocumentWriteOperationImpl(
            sourceDocument.getUri(), sourceDocument.getMetadata(), new JDOMHandle(doc)
        )).iterator();
    }

    private Iterator<DocumentWriteOperation> writeChunksToSeparateDocument(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        Document doc = new Document();
        Element root = new Element("root");
        doc.addContent(root);
        root.addContent(new Element("source-uri").addContent(sourceDocument.getUri()));
        addChunksToDocument(doc, textSegments);

        // Temporary URI and permissions, will make these nicer later.
        String uri = sourceDocument.getUri() + "-chunks.xml";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.getPermissions().addFromDelimitedString("spark-user-role,read,spark-user-role,update");
        return Stream.of(
            sourceDocument,
            new DocumentWriteOperationImpl(uri, metadata, new JDOMHandle(doc))
        ).iterator();
    }

    private void addChunksToDocument(Document doc, List<TextSegment> textSegments) {
        final Element chunks = new Element("chunks");
        doc.getRootElement().addContent(chunks);
        textSegments.forEach(textSegment -> chunks
            .addContent(new Element("chunk").addContent(new Element("text").addContent(textSegment.text())))
        );
    }
}
