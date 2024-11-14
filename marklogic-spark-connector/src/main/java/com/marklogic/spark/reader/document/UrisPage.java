/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractReadHandle;
import com.marklogic.client.io.marker.DocumentMetadataReadHandle;

import java.util.Iterator;

/**
 * Used for streaming documents from MarkLogic to files. Allows {@code ForestReader} to avoid the additional call to
 * MarkLogic to retrieve documents - which we don't want in the reader phase when streaming - while still depending
 * on {@code DocumentPage} and {@code DocumentRecord} as abstractions.
 */
class UrisPage implements DocumentPage {

    private Iterator<String> uris;

    UrisPage(Iterator<String> uris) {
        this.uris = uris;
    }

    @Override
    public <T extends AbstractReadHandle> T nextContent(T contentHandle) {
        return null;
    }

    @Override
    public void close() {
        // Nothing to do here.
    }

    @Override
    public Iterator<DocumentRecord> iterator() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return uris.hasNext();
    }

    @Override
    public DocumentRecord next() {
        // This is the only method that ForestReader will invoke.
        return new UriRecord(uris.next());
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getPageSize() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long getTotalPages() {
        return 0;
    }

    @Override
    public boolean hasContent() {
        return false;
    }

    @Override
    public boolean hasNextPage() {
        return false;
    }

    @Override
    public boolean hasPreviousPage() {
        return false;
    }

    @Override
    public long getPageNumber() {
        return 0;
    }

    @Override
    public boolean isFirstPage() {
        return false;
    }

    @Override
    public boolean isLastPage() {
        return false;
    }

    private static class UriRecord implements DocumentRecord {

        private String uri;

        public UriRecord(String uri) {
            this.uri = uri;
        }

        @Override
        public String getUri() {
            return uri;
        }

        @Override
        public DocumentDescriptor getDescriptor() {
            return null;
        }

        @Override
        public Format getFormat() {
            return null;
        }

        @Override
        public String getMimetype() {
            return null;
        }

        @Override
        public long getLength() {
            return 0;
        }

        @Override
        public <T extends DocumentMetadataReadHandle> T getMetadata(T metadataHandle) {
            return null;
        }

        @Override
        public <T> T getMetadataAs(Class<T> as) {
            return null;
        }

        @Override
        public <T extends AbstractReadHandle> T getContent(T contentHandle) {
            return null;
        }

        @Override
        public <T> T getContentAs(Class<T> as) {
            return null;
        }
    }
}
