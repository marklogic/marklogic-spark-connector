package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

class ReadMlcpArchiveFilesTest extends AbstractIntegrationTest {

    @Test
    void readMlcpArchiveFile() {
        Dataset<Row> reader = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/sample-XML.zip");
        verifyFileRows(reader.collectAsList());
    }

    private void verifyFileRows(List<Row> rows) {
        HashSet<String> hashSet = new HashSet<>();
        hashSet.add("/alexander");
        hashSet.add("/alexander.metadata");
        hashSet.add("/betsy");
        hashSet.add("/betsy.metadata");
        hashSet.add("/george");
        hashSet.add("/george.metadata");
        hashSet.add("/martha");
        hashSet.add("/martha.metadata");
        hashSet.add("/mary");
        hashSet.add("/mary.metadata");

        for(int i =0; i<10; i++){
            String temp = rows.get(i).getString(0).substring(rows.get(i).getString(0).lastIndexOf("/"));
            assert(hashSet.contains(temp));
            hashSet.remove(temp);
        }
        assert(hashSet.isEmpty());
    }
}
