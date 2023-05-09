package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;

import java.util.UUID;

/**
 * This is intended to migrate to java-client-api and likely just be a Builder class on DocumentWriteOperation.
 */
class DocBuilderFactory {

    private DocumentMetadataHandle metadata;
    private DocumentWriteOperation.DocumentUriMaker uriMaker;

    DocBuilderFactory() {
        this.metadata = new DocumentMetadataHandle();
    }

    DocBuilder newDocBuilder() {
        return new DocBuilder(uriMaker, metadata);
    }

    DocBuilderFactory withCollections(String collections) {
        if (collections != null && collections.trim().length() > 0) {
            metadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        DocumentMetadataHandle.DocumentPermissions permissions = metadata.getPermissions();
        // TODO Copied from ml-javaclient-util. Should be in java-client-api.
        if (permissionsString != null && permissionsString.trim().length() > 0) {
            String[] tokens = permissionsString.split(",");
            for (int i = 0; i < tokens.length; i += 2) {
                String role = tokens[i];
                if (i + 1 >= tokens.length) {
                    throw new IllegalArgumentException("Unable to parse permissions string, which must be a comma-separated " +
                        "list of role names and capabilities - i.e. role1,read,role2,update,role3,execute; string: " + permissionsString);
                }
                DocumentMetadataHandle.Capability c;
                try {
                    c = DocumentMetadataHandle.Capability.getValueOf(tokens[i + 1]);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to parse permissions string: " + permissionsString + "; cause: " + e.getMessage());
                }
                if (permissions.containsKey(role)) {
                    permissions.get(role).add(c);
                } else {
                    permissions.add(role, c);
                }
            }
        }
        return this;
    }

    DocBuilderFactory withSimpleUriStrategy(String prefix, String suffix) {
        return withUriMaker(contentHandle -> {
            String uri = "";
            if (prefix != null) {
                uri += prefix;
            }
            uri += UUID.randomUUID().toString();
            return suffix != null ? uri + suffix : uri;
        });
    }

    DocBuilderFactory withUriMaker(DocumentWriteOperation.DocumentUriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }
}
