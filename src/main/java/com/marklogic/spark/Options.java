/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark;

public abstract class Options {

    public static final String CLIENT_AUTH_TYPE = "spark.marklogic.client.authType";
    public static final String CLIENT_CONNECTION_TYPE = "spark.marklogic.client.connectionType";
    public static final String CLIENT_DATABASE = "spark.marklogic.client.database";
    public static final String CLIENT_HOST = "spark.marklogic.client.host";
    public static final String CLIENT_PASSWORD = "spark.marklogic.client.password";
    public static final String CLIENT_PORT = "spark.marklogic.client.port";
    public static final String CLIENT_SSL_ENABLED = "spark.marklogic.client.sslEnabled";
    public static final String CLIENT_URI = "spark.marklogic.client.uri";
    public static final String CLIENT_USERNAME = "spark.marklogic.client.username";

    public static final String READ_INVOKE = "spark.marklogic.read.invoke";
    public static final String READ_JAVASCRIPT = "spark.marklogic.read.javascript";
    public static final String READ_JAVASCRIPT_FILE = "spark.marklogic.read.javascriptFile";
    public static final String READ_XQUERY = "spark.marklogic.read.xquery";
    public static final String READ_XQUERY_FILE = "spark.marklogic.read.xqueryFile";
    public static final String READ_VARS_PREFIX = "spark.marklogic.read.vars.";

    public static final String READ_PARTITIONS_INVOKE = "spark.marklogic.read.partitions.invoke";
    public static final String READ_PARTITIONS_JAVASCRIPT = "spark.marklogic.read.partitions.javascript";
    public static final String READ_PARTITIONS_JAVASCRIPT_FILE = "spark.marklogic.read.partitions.javascriptFile";
    public static final String READ_PARTITIONS_XQUERY = "spark.marklogic.read.partitions.xquery";
    public static final String READ_PARTITIONS_XQUERY_FILE = "spark.marklogic.read.partitions.xqueryFile";

    public static final String READ_OPTIC_QUERY = "spark.marklogic.read.opticQuery";
    public static final String READ_NUM_PARTITIONS = "spark.marklogic.read.numPartitions";
    public static final String READ_BATCH_SIZE = "spark.marklogic.read.batchSize";
    public static final String READ_PUSH_DOWN_AGGREGATES = "spark.marklogic.read.pushDownAggregates";

    // "categories" as defined by https://docs.marklogic.com/REST/GET/v1/documents .
    public static final String READ_DOCUMENTS_CATEGORIES = "spark.marklogic.read.documents.categories";
    public static final String READ_DOCUMENTS_COLLECTIONS = "spark.marklogic.read.documents.collections";
    public static final String READ_DOCUMENTS_DIRECTORY = "spark.marklogic.read.documents.directory";
    public static final String READ_DOCUMENTS_FILTERED = "spark.marklogic.read.documents.filtered";
    public static final String READ_DOCUMENTS_OPTIONS = "spark.marklogic.read.documents.options";
    public static final String READ_DOCUMENTS_PARTITIONS_PER_FOREST = "spark.marklogic.read.documents.partitionsPerForest";
    // Corresponds to "q" at https://docs.marklogic.com/REST/POST/v1/search, known as a "string query".
    // Corresponds to the complex query submitted via the request body at https://docs.marklogic.com/REST/POST/v1/search .
    public static final String READ_DOCUMENTS_QUERY = "spark.marklogic.read.documents.query";
    public static final String READ_DOCUMENTS_STRING_QUERY = "spark.marklogic.read.documents.stringQuery";
    public static final String READ_DOCUMENTS_TRANSFORM = "spark.marklogic.read.documents.transform";
    public static final String READ_DOCUMENTS_TRANSFORM_PARAMS = "spark.marklogic.read.documents.transformParams";
    public static final String READ_DOCUMENTS_TRANSFORM_PARAMS_DELIMITER = "spark.marklogic.read.documents.transformParamsDelimiter";
    public static final String READ_DOCUMENTS_URIS = "spark.marklogic.read.documents.uris";

    public static final String READ_TRIPLES_GRAPHS = "spark.marklogic.read.triples.graphs";
    public static final String READ_TRIPLES_COLLECTIONS = "spark.marklogic.read.triples.collections";
    public static final String READ_TRIPLES_QUERY = "spark.marklogic.read.triples.query";
    public static final String READ_TRIPLES_STRING_QUERY = "spark.marklogic.read.triples.stringQuery";
    public static final String READ_TRIPLES_URIS = "spark.marklogic.read.triples.uris";
    public static final String READ_TRIPLES_DIRECTORY = "spark.marklogic.read.triples.directory";
    public static final String READ_TRIPLES_OPTIONS = "spark.marklogic.read.triples.options";
    public static final String READ_TRIPLES_FILTERED = "spark.marklogic.read.triples.filtered";
    public static final String READ_TRIPLES_BASE_IRI = "spark.marklogic.read.triples.baseIri";

    public static final String READ_FILES_TYPE = "spark.marklogic.read.files.type";
    public static final String READ_FILES_COMPRESSION = "spark.marklogic.read.files.compression";
    public static final String READ_FILES_ABORT_ON_FAILURE = "spark.marklogic.read.files.abortOnFailure";
    public static final String READ_ARCHIVES_CATEGORIES = "spark.marklogic.read.archives.categories";

    // "Aggregate" = an XML document containing N child elements, each of which should become a row / document.
    // "xml" is included in the name in anticipation of eventually supporting "aggregate JSON" - i.e. an array of N
    // objects, where each object should become a row / document (this is different from JSON lines format).
    public static final String READ_AGGREGATES_XML_ELEMENT = "spark.marklogic.read.aggregates.xml.element";
    public static final String READ_AGGREGATES_XML_NAMESPACE = "spark.marklogic.read.aggregates.xml.namespace";
    public static final String READ_AGGREGATES_XML_URI_ELEMENT = "spark.marklogic.read.aggregates.xml.uriElement";
    public static final String READ_AGGREGATES_XML_URI_NAMESPACE = "spark.marklogic.read.aggregates.xml.uriNamespace";

    public static final String WRITE_BATCH_SIZE = "spark.marklogic.write.batchSize";
    public static final String WRITE_THREAD_COUNT = "spark.marklogic.write.threadCount";
    public static final String WRITE_ABORT_ON_FAILURE = "spark.marklogic.write.abortOnFailure";

    // For writing via custom code.
    public static final String WRITE_INVOKE = "spark.marklogic.write.invoke";
    public static final String WRITE_JAVASCRIPT = "spark.marklogic.write.javascript";
    public static final String WRITE_JAVASCRIPT_FILE = "spark.marklogic.write.javascriptFile";
    public static final String WRITE_XQUERY = "spark.marklogic.write.xquery";
    public static final String WRITE_XQUERY_FILE = "spark.marklogic.write.xqueryFile";
    public static final String WRITE_EXTERNAL_VARIABLE_NAME = "spark.marklogic.write.externalVariableName";
    public static final String WRITE_EXTERNAL_VARIABLE_DELIMITER = "spark.marklogic.write.externalVariableDelimiter";
    public static final String WRITE_VARS_PREFIX = "spark.marklogic.write.vars.";

    // For writing documents to MarkLogic.
    public static final String WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS = "spark.marklogic.write.archivePathForFailedDocuments";
    public static final String WRITE_COLLECTIONS = "spark.marklogic.write.collections";
    public static final String WRITE_PERMISSIONS = "spark.marklogic.write.permissions";
    public static final String WRITE_JSON_ROOT_NAME = "spark.marklogic.write.jsonRootName";
    public static final String WRITE_TEMPORAL_COLLECTION = "spark.marklogic.write.temporalCollection";
    public static final String WRITE_URI_PREFIX = "spark.marklogic.write.uriPrefix";
    public static final String WRITE_URI_REPLACE = "spark.marklogic.write.uriReplace";
    public static final String WRITE_URI_SUFFIX = "spark.marklogic.write.uriSuffix";
    public static final String WRITE_URI_TEMPLATE = "spark.marklogic.write.uriTemplate";
    public static final String WRITE_TRANSFORM_NAME = "spark.marklogic.write.transform";
    public static final String WRITE_TRANSFORM_PARAMS = "spark.marklogic.write.transformParams";
    public static final String WRITE_TRANSFORM_PARAMS_DELIMITER = "spark.marklogic.write.transformParamsDelimiter";
    public static final String WRITE_XML_ROOT_NAME = "spark.marklogic.write.xmlRootName";
    public static final String WRITE_XML_NAMESPACE = "spark.marklogic.write.xmlNamespace";

    // For writing RDF
    public static final String WRITE_GRAPH = "spark.marklogic.write.graph";
    public static final String WRITE_GRAPH_OVERRIDE = "spark.marklogic.write.graphOverride";

    // For writing rows adhering to Spark's binaryFile schema - https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html .
    // This was introduced in 2.2.0 and unfortunately uses "fileRows" instead of "binaryFileRows".
    public static final String WRITE_FILE_ROWS_DOCUMENT_TYPE = "spark.marklogic.write.fileRows.documentType";

    // For writing rows adhering to {@code DocumentRowSchema} or {@code TripleRowSchema} to a filesystem.
    public static final String WRITE_FILES_COMPRESSION = "spark.marklogic.write.files.compression";

    // Applies to XML and JSON documents.
    public static final String WRITE_FILES_PRETTY_PRINT = "spark.marklogic.write.files.prettyPrint";

    public static final String WRITE_RDF_FILES_FORMAT = "spark.marklogic.write.files.rdf.format";
    public static final String WRITE_RDF_FILES_GRAPH = "spark.marklogic.write.files.rdf.graph";

    private Options() {
    }
}
