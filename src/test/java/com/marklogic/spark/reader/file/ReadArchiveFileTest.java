package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReadArchiveFileTest extends AbstractIntegrationTest {

    @BeforeEach
    void beforeEach() {
        TestUtil.insertTwoDocumentsWithAllMetadata(getDatabaseClient());
    }

    @Test
    void test(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyFileRows(tempDir, 2);
    }

    private void verifyFileRows(Path tempDir, int rowCount) {
        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(rowCount, rows.size(), "Expecting 2 rows in the zip.");

        for(int i=0; i<rowCount; i++){
            Row row = rows.get(i);
            assertTrue(row.getString(0).endsWith("/test/"+(i+1)+".xml"));

            String content = new String((byte[]) row.get(1), StandardCharsets.UTF_8);
            assertTrue(content.contains("<hello>world</hello>"));

            assertEquals("XML", row.get(2));

            List<String> collections = JavaConversions.seqAsJavaList(row.getSeq(3));
            assertEquals("collection1", collections.get(0));
            assertEquals("collection2", collections.get(1));

            Map<String, WrappedArray> permissions = row.getJavaMap(4);
            assertTrue(permissions.get("spark-user-role").toString().contains("READ"));
            assertTrue(permissions.get("spark-user-role").toString().contains("UPDATE"));
            assertTrue(permissions.get("qconsole-user").toString().contains("READ"));

            assertEquals(10, row.get(5));

            Map<String, String> properties = row.getJavaMap(6);
            assertEquals("value2", properties.get("key2"));
            assertEquals("value1", properties.get("{org:example}key1"));

            Map<String, String> metadataValues = row.getJavaMap(7);
            assertEquals("value1", metadataValues.get("meta1"));
            assertEquals("value2", metadataValues.get("meta2"));
        }
    }
}
