/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles converting an MLCP metadata document, generated when creating an MLCP archive, into a
 * {@code DocumentMetadataHandle} instance, thus allowing it to be reused with the REST API. The MLCP metadata
 * document is expected to have a root element of "com.marklogic.contentpump.DocumentMetadata".
 */
class MlcpMetadataConverter {

    private static final Namespace SECURITY_NAMESPACE = Namespace.getNamespace("sec", "http://marklogic.com/xdmp/security");
    private final SAXBuilder saxBuilder;

    MlcpMetadataConverter() {
        this.saxBuilder = new SAXBuilder();
    }

    MlcpMetadata convert(InputStream inputStream) throws JDOMException, IOException {
        Document doc = this.saxBuilder.build(inputStream);
        Element mlcpMetadata = doc.getRootElement();
        Element properties = mlcpMetadata.getChild("properties");

        DocumentMetadataHandle restMetadata = properties != null ?
            newMetadataWithProperties(properties.getText()) :
            new DocumentMetadataHandle();

        addCollections(mlcpMetadata, restMetadata);
        addPermissions(mlcpMetadata, restMetadata);
        addQuality(mlcpMetadata, restMetadata);
        addMetadataValues(mlcpMetadata, restMetadata);

        Format javaFormat = getFormat(mlcpMetadata);
        return new MlcpMetadata(restMetadata, javaFormat);
    }

    private Format getFormat(Element mlcpMetadata) {
        Element format = mlcpMetadata.getChild("format");
        if (format != null && format.getChild("name") != null) {
            String value = format.getChildText("name");
            // MLCP uses "text()" for an unknown reason.
            if (value.startsWith("text")) {
                value = "text";
            }
            return Format.valueOf(value.toUpperCase());
        }
        return null;
    }

    /**
     * This allows for the logic in DocumentMetadataHandle for parsing the properties fragment to be reused.
     *
     * @param propertiesXml
     * @return
     */
    private DocumentMetadataHandle newMetadataWithProperties(String propertiesXml) {
        String restXml = String.format("<rapi:metadata xmlns:rapi='http://marklogic.com/rest-api'>%s</rapi:metadata>", propertiesXml);
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.fromBuffer(restXml.getBytes());
        return metadata;
    }

    private void addCollections(Element mlcpMetadata, DocumentMetadataHandle restMetadata) {
        Element collections = mlcpMetadata.getChild("collectionsList");
        if (collections != null) {
            for (Element string : collections.getChildren("string")) {
                restMetadata.getCollections().add(string.getText());
            }
        }
    }

    private void addQuality(Element mlcpMetadata, DocumentMetadataHandle restMetadata) {
        Element quality = mlcpMetadata.getChild("quality");
        if (quality != null) {
            restMetadata.setQuality(Integer.parseInt(quality.getText()));
        }
    }

    private void addMetadataValues(Element mlcpMetadata, DocumentMetadataHandle restMetadata) {
        Element collections = mlcpMetadata.getChild("meta");
        if (collections != null) {
            for (Element entry : collections.getChildren("entry")) {
                List<Element> strings = entry.getChildren("string");
                String key = strings.get(0).getText();
                String value = strings.get(1).getText();
                restMetadata.getMetadataValues().put(key, value);
            }
        }
    }

    /**
     * The "permissionsList" element can contain permissions that have references to other permissions, where it's
     * not clear which permission it's referring to. For example:
     * <p>
     * <com.marklogic.xcc.ContentPermission>
     * <capability reference="../../com.marklogic.xcc.ContentPermission/capability"/>
     * <p>
     * It this does not seem reliably to determine the capability of each permission based on each
     * com.marklogic.xcc.ContentPermission element. But each such element does associate a roleId and a roleName
     * together. The value of the "permString" element, which associates roleIds and capabilities, can then be used as
     * a reliable source of permission information, with each roleId being bounced against a map of roleId -> roleName.
     *
     * @param mlcpMetadata
     * @param restMetadata
     * @throws IOException
     * @throws JDOMException
     */
    private void addPermissions(Element mlcpMetadata, DocumentMetadataHandle restMetadata) throws IOException, JDOMException {
        Element permString = mlcpMetadata.getChild("permString");
        if (permString == null) {
            return;
        }

        Element permissionsList = mlcpMetadata.getChild("permissionsList");
        if (permissionsList == null) {
            return;
        }

        Map<String, String> roleIdsToNames = buildRoleMap(permissionsList);

        Element perms = this.saxBuilder.build(new StringReader(permString.getText())).getRootElement();
        for (Element perm : perms.getChildren("permission", SECURITY_NAMESPACE)) {
            String capability = perm.getChildText("capability", SECURITY_NAMESPACE);
            DocumentMetadataHandle.Capability cap = DocumentMetadataHandle.Capability.valueOf(capability.toUpperCase());
            String roleId = perm.getChildText("role-id", SECURITY_NAMESPACE);
            String roleName = roleIdsToNames.get(roleId);
            if (restMetadata.getPermissions().containsKey(roleName)) {
                restMetadata.getPermissions().get(roleName).add(cap);
            } else {
                restMetadata.getPermissions().add(roleName, cap);
            }
        }
    }

    /**
     * @param permissionsList
     * @return a map of roleId -> roleName based on the role and role elements in each
     * com.marklogic.xcc.ContentPermission element.
     */
    private Map<String, String> buildRoleMap(Element permissionsList) {
        Map<String, String> roleIdsToNames = new HashMap<>();
        for (Element permission : permissionsList.getChildren("com.marklogic.xcc.ContentPermission")) {
            String role = permission.getChildText("role");
            String roleId = permission.getChildText("roleId");
            roleIdsToNames.put(roleId, role);
        }
        return roleIdsToNames;
    }
}
