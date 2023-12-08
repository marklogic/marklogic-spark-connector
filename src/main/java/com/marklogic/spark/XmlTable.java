package com.marklogic.spark;

import com.marklogic.spark.reader.ReadContext;
import com.marklogic.spark.reader.XmlScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class XmlTable implements SupportsRead {

    private static final Set<TableCapability> capabilities;

    private StructType readSchema;
    private Map<String, String> readProperties;

    static {
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
    }

    XmlTable(StructType schema, Map<String, String> properties) {
        this.readSchema = schema;
        this.readProperties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new XmlScanBuilder(new ReadContext(readProperties, readSchema));
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public StructType schema() {
        return readSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
