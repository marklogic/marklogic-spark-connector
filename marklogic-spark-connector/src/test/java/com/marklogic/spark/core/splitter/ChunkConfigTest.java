/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.io.DocumentMetadataHandle;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChunkConfigTest {

    @Test
    void defaultBehaviorNoInheritance() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getCollections().add("explicit-chunk-col");
        explicitMetadata.getPermissions().add("spark-user-role", DocumentMetadataHandle.Capability.READ);

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .build();

        assertFalse(config.isInheritCollections());
        assertFalse(config.isInheritPermissions());

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getCollections().addAll("src-col-1", "src-col-2");
        sourceMetadata.getPermissions().add("manage-user", DocumentMetadataHandle.Capability.UPDATE);

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);
        assertSame(explicitMetadata, chunkMetadata, "When neither inheritance option is enabled, static metadata is returned");
        assertEquals(1, chunkMetadata.getCollections().size());
        assertTrue(chunkMetadata.getCollections().contains("explicit-chunk-col"));
        assertEquals(1, chunkMetadata.getPermissions().size());
        assertTrue(chunkMetadata.getPermissions().containsKey("spark-user-role"));
        assertFalse(chunkMetadata.getPermissions().containsKey("manage-user"));
    }

    @Test
    void inheritCollectionsOnly() {
        ChunkConfig config = new ChunkConfig.Builder()
            .withInheritCollections(true)
            .build();

        assertTrue(config.isInheritCollections());
        assertFalse(config.isInheritPermissions());

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getCollections().addAll("src-1", "src-2");

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);
        assertEquals(2, chunkMetadata.getCollections().size());
        assertTrue(chunkMetadata.getCollections().contains("src-1"));
        assertTrue(chunkMetadata.getCollections().contains("src-2"));
        assertTrue(chunkMetadata.getPermissions().isEmpty());
    }

    @Test
    void inheritCollectionsUnionWithExplicitCollections() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getCollections().addAll("shared-col", "explicit-col");

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .withInheritCollections(true)
            .build();

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getCollections().addAll("shared-col", "src-col");

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);
        assertEquals(3, chunkMetadata.getCollections().size(), "Collections should be deduplicated union of source and explicit");
        assertTrue(chunkMetadata.getCollections().contains("shared-col"));
        assertTrue(chunkMetadata.getCollections().contains("src-col"));
        assertTrue(chunkMetadata.getCollections().contains("explicit-col"));
    }

    @Test
    void inheritCollectionsWithEmptyOrNullSource() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getCollections().add("explicit-col");

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .withInheritCollections(true)
            .build();

        // Null source
        DocumentMetadataHandle chunkMetadataNull = config.buildChunkMetadata(null);
        assertEquals(1, chunkMetadataNull.getCollections().size());
        assertTrue(chunkMetadataNull.getCollections().contains("explicit-col"));

        // Empty source
        DocumentMetadataHandle emptySource = new DocumentMetadataHandle();
        DocumentMetadataHandle chunkMetadataEmpty = config.buildChunkMetadata(emptySource);
        assertEquals(1, chunkMetadataEmpty.getCollections().size());
        assertTrue(chunkMetadataEmpty.getCollections().contains("explicit-col"));

        // No explicit, empty source -> clean no-op
        ChunkConfig configNoExplicit = new ChunkConfig.Builder()
            .withInheritCollections(true)
            .build();
        DocumentMetadataHandle chunkMetadataNoop = configNoExplicit.buildChunkMetadata(emptySource);
        assertTrue(chunkMetadataNoop.getCollections().isEmpty());
    }

    @Test
    void inheritPermissionsOnly() {
        ChunkConfig config = new ChunkConfig.Builder()
            .withInheritPermissions(true)
            .build();

        assertTrue(config.isInheritPermissions());
        assertFalse(config.isInheritCollections());

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getPermissions().add("role-a", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);
        assertEquals(1, chunkMetadata.getPermissions().size());
        assertTrue(chunkMetadata.getPermissions().get("role-a").contains(DocumentMetadataHandle.Capability.READ));
        assertTrue(chunkMetadata.getPermissions().get("role-a").contains(DocumentMetadataHandle.Capability.UPDATE));
        assertTrue(chunkMetadata.getCollections().isEmpty());
    }

    @Test
    void inheritPermissionsUnionWithExplicitPermissions() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getPermissions().add("role-a", DocumentMetadataHandle.Capability.UPDATE);
        explicitMetadata.getPermissions().add("role-b", DocumentMetadataHandle.Capability.READ);

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .withInheritPermissions(true)
            .build();

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getPermissions().add("role-a", DocumentMetadataHandle.Capability.READ);
        sourceMetadata.getPermissions().add("role-c", DocumentMetadataHandle.Capability.EXECUTE);

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);
        assertEquals(3, chunkMetadata.getPermissions().size());

        // role-a has union of READ and UPDATE
        assertEquals(2, chunkMetadata.getPermissions().get("role-a").size());
        assertTrue(chunkMetadata.getPermissions().get("role-a").contains(DocumentMetadataHandle.Capability.READ));
        assertTrue(chunkMetadata.getPermissions().get("role-a").contains(DocumentMetadataHandle.Capability.UPDATE));

        // role-b from explicit
        assertEquals(1, chunkMetadata.getPermissions().get("role-b").size());
        assertTrue(chunkMetadata.getPermissions().get("role-b").contains(DocumentMetadataHandle.Capability.READ));

        // role-c from source
        assertEquals(1, chunkMetadata.getPermissions().get("role-c").size());
        assertTrue(chunkMetadata.getPermissions().get("role-c").contains(DocumentMetadataHandle.Capability.EXECUTE));
    }

    @Test
    void inheritPermissionsWithEmptyOrNullSource() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getPermissions().add("role-explicit", DocumentMetadataHandle.Capability.READ);

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .withInheritPermissions(true)
            .build();

        // Null source -> only explicit applied
        DocumentMetadataHandle chunkMetadataNull = config.buildChunkMetadata(null);
        assertEquals(1, chunkMetadataNull.getPermissions().size());
        assertTrue(chunkMetadataNull.getPermissions().containsKey("role-explicit"));

        // Empty source -> only explicit applied
        DocumentMetadataHandle emptySource = new DocumentMetadataHandle();
        DocumentMetadataHandle chunkMetadataEmpty = config.buildChunkMetadata(emptySource);
        assertEquals(1, chunkMetadataEmpty.getPermissions().size());
        assertTrue(chunkMetadataEmpty.getPermissions().containsKey("role-explicit"));

        // No explicit, empty source -> clean no-op
        ChunkConfig configNoExplicit = new ChunkConfig.Builder()
            .withInheritPermissions(true)
            .build();
        DocumentMetadataHandle chunkMetadataNoop = configNoExplicit.buildChunkMetadata(emptySource);
        assertTrue(chunkMetadataNoop.getPermissions().isEmpty());
    }

    @Test
    void inheritBothCollectionsAndPermissions() {
        DocumentMetadataHandle explicitMetadata = new DocumentMetadataHandle();
        explicitMetadata.getCollections().add("explicit-col");
        explicitMetadata.getPermissions().add("role-explicit", DocumentMetadataHandle.Capability.UPDATE);

        ChunkConfig config = new ChunkConfig.Builder()
            .withMetadata(explicitMetadata)
            .withInheritCollections(true)
            .withInheritPermissions(true)
            .build();

        DocumentMetadataHandle sourceMetadata = new DocumentMetadataHandle();
        sourceMetadata.getCollections().add("src-col");
        sourceMetadata.getPermissions().add("role-src", DocumentMetadataHandle.Capability.READ);

        DocumentMetadataHandle chunkMetadata = config.buildChunkMetadata(sourceMetadata);

        // Collections union
        assertEquals(2, chunkMetadata.getCollections().size());
        assertTrue(chunkMetadata.getCollections().contains("explicit-col"));
        assertTrue(chunkMetadata.getCollections().contains("src-col"));

        // Permissions union
        assertEquals(2, chunkMetadata.getPermissions().size());
        assertTrue(chunkMetadata.getPermissions().containsKey("role-explicit"));
        assertTrue(chunkMetadata.getPermissions().containsKey("role-src"));
    }
}
