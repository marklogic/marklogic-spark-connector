/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

    /**
     * Alias for "spark.marklogic.client.uri", which will be deprecated soon in favor of this better name.
     *
     * @since 2.5.1
     */
    public static final String CLIENT_CONNECTION_STRING = "spark.marklogic.client.connectionString";

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
    public static final String READ_OPTIC_PARAM_PREFIX = "spark.marklogic.read.optic.param.";

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

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_INVOKE = "spark.marklogic.read.secondaryUris.invoke";

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_JAVASCRIPT = "spark.marklogic.read.secondaryUris.javascript";

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_JAVASCRIPT_FILE = "spark.marklogic.read.secondaryUris.javascriptFile";

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_XQUERY = "spark.marklogic.read.secondaryUris.xquery";

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_XQUERY_FILE = "spark.marklogic.read.secondaryUris.xqueryFile";

    /**
     * @since 2.7.0
     */
    public static final String READ_SECONDARY_URIS_VARS_PREFIX = "spark.marklogic.read.secondaryUris.vars.";

    public static final String READ_TRIPLES_GRAPHS = "spark.marklogic.read.triples.graphs";
    public static final String READ_TRIPLES_COLLECTIONS = "spark.marklogic.read.triples.collections";
    public static final String READ_TRIPLES_QUERY = "spark.marklogic.read.triples.query";
    public static final String READ_TRIPLES_STRING_QUERY = "spark.marklogic.read.triples.stringQuery";
    public static final String READ_TRIPLES_URIS = "spark.marklogic.read.triples.uris";
    public static final String READ_TRIPLES_DIRECTORY = "spark.marklogic.read.triples.directory";
    public static final String READ_TRIPLES_OPTIONS = "spark.marklogic.read.triples.options";
    public static final String READ_TRIPLES_FILTERED = "spark.marklogic.read.triples.filtered";
    public static final String READ_TRIPLES_BASE_IRI = "spark.marklogic.read.triples.baseIri";

    /**
     * The connector uses a consistent snapshot by default. Setting this to false results in queries being executed
     * at multiple points of time, potentially yielding inconsistent results.
     *
     * @since 2.4.2
     */
    public static final String READ_SNAPSHOT = "spark.marklogic.read.snapshot";

    // For logging progress when reading documents, rows, or items via custom code. Defines the interval at which
    // progress should be logged - e.g. a value of 10,000 will result in a message being logged on every 10,000 items
    // being written/processed.
    public static final String READ_LOG_PROGRESS = "spark.marklogic.read.logProgress";

    public static final String READ_FILES_TYPE = "spark.marklogic.read.files.type";
    public static final String READ_FILES_COMPRESSION = "spark.marklogic.read.files.compression";
    public static final String READ_FILES_ENCODING = "spark.marklogic.read.files.encoding";
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
    public static final String WRITE_THREAD_COUNT_PER_PARTITION = "spark.marklogic.write.threadCountPerPartition";
    public static final String WRITE_ABORT_ON_FAILURE = "spark.marklogic.write.abortOnFailure";

    // For logging progress when writing documents or processing with custom code. Defines the interval at which
    // progress should be logged - e.g. a value of 10,000 will result in a message being logged on every 10,000 items
    // being written/processed.
    public static final String WRITE_LOG_PROGRESS = "spark.marklogic.write.logProgress";

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

    /**
     * @since 2.6.0
     */
    public static final String WRITE_METADATA_VALUES_PREFIX = "spark.marklogic.write.metadataValues.";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_DOCUMENT_PROPERTIES_PREFIX = "spark.marklogic.write.documentProperties.";

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

    // For serializing a row into JSON. Intent is to allow for other constants defined in the Spark
    // JSONOptions.scala class to be used after "spark.marklogic.write.json."
    // Example - "spark.marklogic.write.json.ignoreNullFields=false.
    public static final String WRITE_JSON_SERIALIZATION_OPTION_PREFIX = "spark.marklogic.write.json.";

    /**
     * Defines the number of rows to batch up before sending them through the optional document pipeline process.
     * Defaults to 1.
     *
     * @since 2.6.0
     */
    public static final String WRITE_PIPELINE_BATCH_SIZE = "spark.marklogic.write.pipeline.batchSize";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_EXTRACTED_TEXT = "spark.marklogic.write.extractedText";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE = "spark.marklogic.write.extractedText.documentType";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_EXTRACTED_TEXT_COLLECTIONS = "spark.marklogic.write.extractedText.collections";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_EXTRACTED_TEXT_PERMISSIONS = "spark.marklogic.write.extractedText.permissions";

    /**
     * @since 2.6.0
     */
    public static final String WRITE_EXTRACTED_TEXT_DROP_SOURCE = "spark.marklogic.write.extractedText.dropSource";

    public static final String WRITE_SPLITTER_PREFIX = "spark.marklogic.write.splitter.";

    /**
     * Enables the splitter feature by defining an XPath expression for selecting text to split in a document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_XPATH = WRITE_SPLITTER_PREFIX + "xpath";

    /**
     * Enables the splitter feature by defining one or more newline-delimited JSON Pointer expressions for selecting
     * text to split in a document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_JSON_POINTERS = WRITE_SPLITTER_PREFIX + "jsonPointers";

    /**
     * Enables the splitter feature by declaring that all the text in a document should be split. This is typically for
     * text documents, but can be used for JSON and XML as well.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_TEXT = WRITE_SPLITTER_PREFIX + "text";

    /**
     * Defines the maximum chunk size in characters. Defaults to 1000.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_MAX_CHUNK_SIZE = WRITE_SPLITTER_PREFIX + "maxChunkSize";

    /**
     * Defines the maximum overlap size in characters between two chunks. Defaults to 0.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_MAX_OVERLAP_SIZE = WRITE_SPLITTER_PREFIX + "maxOverlapSize";

    /**
     * Defines a regex for splitting text into chunks. The default strategy is LangChain4J's "recursive" strategy that
     * splits on paragraphs, sentences, lines, and words.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_REGEX = WRITE_SPLITTER_PREFIX + "regex";

    /**
     * Defines a delimiter for usage with the splitter regex option. The delimiter joins together two or more chunks
     * identified via the regex to produce a chunk that is as close as possible to the maximum chunk size.
     *
     * @since 2.5.0
     */
    // This mistakenly omitted "write" in the option name. Will need to be deprecated and replaced.
    public static final String WRITE_SPLITTER_JOIN_DELIMITER = "spark.marklogic.splitter.joinDelimiter";

    /**
     * Defines the class name of an implementation of LangChain4J's {@code dev.langchain4j.data.document.DocumentSplitter}
     * interface to be used for splitting the selected text into chunks.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_CUSTOM_CLASS = WRITE_SPLITTER_PREFIX + "customClass";

    /**
     * Prefix for one or more options to pass in a {@code Map<String, String>} to the constructor of the custom splitter
     * class.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_CUSTOM_CLASS_OPTION_PREFIX = WRITE_SPLITTER_PREFIX + "customClass.option.";

    /**
     * Configures the connector to write chunks to separate "sidecar" documents instead of to the source document (the
     * default behavior). Defines the maximum number of chunks to write to a sidecar document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_MAX_CHUNKS = WRITE_SPLITTER_PREFIX + "sidecar.maxChunks";

    /**
     * Defines the type - either JSON or XML - of each chunk document. Defaults to the type of the source document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE = WRITE_SPLITTER_PREFIX + "sidecar.documentType";

    /**
     * Comma-delimited list of collections to assign to each chunk document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_COLLECTIONS = WRITE_SPLITTER_PREFIX + "sidecar.collections";

    /**
     * Comma-delimited list of roles and capabilities to assign to each chunk document. If not defined, chunk documents
     * will inherit the permissions defined by {@code WRITE_PERMISSIONS}.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_PERMISSIONS = WRITE_SPLITTER_PREFIX + "sidecar.permissions";

    /**
     * Root name for a JSON or XML sidecar chunk document.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_ROOT_NAME = WRITE_SPLITTER_PREFIX + "sidecar.rootName";

    /**
     * URI prefix for each sidecar chunk document. If defined, will be followed by a UUID.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_URI_PREFIX = WRITE_SPLITTER_PREFIX + "sidecar.uriPrefix";

    /**
     * URI suffix for each sidecar chunk document. If defined, will be preceded by a UUID.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_URI_SUFFIX = WRITE_SPLITTER_PREFIX + "sidecar.uriSuffix";

    /**
     * Namespace for XML sidecar chunk documents.
     *
     * @since 2.5.0
     */
    public static final String WRITE_SPLITTER_SIDECAR_XML_NAMESPACE = WRITE_SPLITTER_PREFIX + "sidecar.xmlNamespace";

    // For writing RDF
    public static final String WRITE_GRAPH = "spark.marklogic.write.graph";
    public static final String WRITE_GRAPH_OVERRIDE = "spark.marklogic.write.graphOverride";

    /**
     * For writing rows adhering to Spark's binaryFile schema - https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html .
     *
     * @deprecated since 2.3.0
     */
    @Deprecated(since = "2.3.0", forRemoval = true)
    // We don't need Sonar to remind us of this deprecation.
    @SuppressWarnings("java:S1133")
    public static final String WRITE_FILE_ROWS_DOCUMENT_TYPE = "spark.marklogic.write.fileRows.documentType";

    // Forces a document type when writing rows corresponding to our document row schema. Used when the URI extension
    // does not result in MarkLogic choosing the correct document type.
    public static final String WRITE_DOCUMENT_TYPE = "spark.marklogic.write.documentType";

    // For writing rows adhering to {@code DocumentRowSchema} or {@code TripleRowSchema} to a filesystem.
    public static final String WRITE_FILES_COMPRESSION = "spark.marklogic.write.files.compression";

    // Applies to XML and JSON documents.
    public static final String WRITE_FILES_PRETTY_PRINT = "spark.marklogic.write.files.prettyPrint";

    // Applies to writing documents as files, gzipped files, and as entries in zips/archives.
    public static final String WRITE_FILES_ENCODING = "spark.marklogic.write.files.encoding";

    public static final String WRITE_RDF_FILES_FORMAT = "spark.marklogic.write.files.rdf.format";
    public static final String WRITE_RDF_FILES_GRAPH = "spark.marklogic.write.files.rdf.graph";

    /**
     * When used in the reader phase while reading generic files, the connector will put a serialized {@code FileContext}
     * into the content column instead of the contents of the file. When used during the writer phase when writing rows
     * conforming to {@code DocumentRowSchema}, the connector will stream the file using the {@code FileContext} to
     * avoid reading its contents into memory.
     * <p>
     * Similarly, when used in the reader phase when reading documents from MarkLogic, the value of the 'content' column
     * in each row will be null. During the writer phase, the connector will retrieve the document corresponding to the
     * value in the 'uri' column and stream it to file.
     *
     * @since 2.4.0
     */
    public static final String STREAM_FILES = "spark.marklogic.streamFiles";

    /**
     * Prefix for registering XML namespace prefixes and URIs that can be reused in any connector
     * feature that accepts an XPath expression.
     *
     * @since 2.5.0
     */
    public static final String XPATH_NAMESPACE_PREFIX = "spark.marklogic.xpath.";

    public static final String WRITE_EMBEDDER_PREFIX = "spark.marklogic.write.embedder.";

    /**
     * Optional prompt used when generating embeddings. The prompt is prepended to the text being embedded.
     *
     * @since 2.7.0
     */
    public static final String WRITE_EMBEDDER_PROMPT = WRITE_EMBEDDER_PREFIX + "prompt";

    /**
     * Enables the embedder feature; name of a class on the classpath that implements the interface
     * {@code Function<Map<String, String>, EmbeddingModel>}.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME = WRITE_EMBEDDER_PREFIX + "modelFunction.className";

    /**
     * Prefix for each option passed in a {@code Map<String, String>} to the {@code apply} method of the model function class.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX = WRITE_EMBEDDER_PREFIX + "modelFunction.option.";

    /**
     * Defines the location of JSON chunks when using the embedder separate from the splitter.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_CHUNKS_JSON_POINTER = WRITE_EMBEDDER_PREFIX + "chunks.jsonPointer";

    /**
     * Defines the location of text in JSON chunks when using the embedder separate from the splitter.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_TEXT_JSON_POINTER = WRITE_EMBEDDER_PREFIX + "text.jsonPointer";

    /**
     * Defines the location of XML chunks when using the embedder separate from the splitter.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_CHUNKS_XPATH = WRITE_EMBEDDER_PREFIX + "chunks.xpath";

    /**
     * Defines the location of text in XML chunks when using the embedder separate from the splitter.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_TEXT_XPATH = WRITE_EMBEDDER_PREFIX + "text.xpath";

    /**
     * Allows for the embedding name to be customized when the embedding is added to a JSON or XML chunk.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_EMBEDDING_NAME = WRITE_EMBEDDER_PREFIX + "embedding.name";

    /**
     * Allows for an optional namespace to be assigned to the embedding element in an XML chunk.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_EMBEDDING_NAMESPACE = WRITE_EMBEDDER_PREFIX + "embedding.namespace";

    /**
     * Defines the number of chunks to send to the embedding model in a single call. Defaults to 1.
     *
     * @since 2.5.0
     */
    public static final String WRITE_EMBEDDER_BATCH_SIZE = WRITE_EMBEDDER_PREFIX + "batchSize";

    /**
     * Enables base64 encoding of vector embeddings in a format supported by the MarkLogic server.
     *
     * @since 2.7.0
     */
    public static final String WRITE_EMBEDDER_BASE64_ENCODE = WRITE_EMBEDDER_PREFIX + "base64Encode";

    /**
     * Defines the host for classification requests
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_HOST = "spark.marklogic.write.classifier.host";

    /**
     * Specifies if the protocol should be http for classification requests.
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_HTTP = "spark.marklogic.write.classifier.http";

    /**
     * Defines the port for classification requests
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_PORT = "spark.marklogic.write.classifier.port";

    /**
     * Defines the endpoint for classification requests
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_PATH = "spark.marklogic.write.classifier.path";

    /**
     * Defines the URL path for generating tokens for classification requests
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_TOKEN_PATH = "spark.marklogic.write.classifier.tokenPath";

    /**
     * Defines the API key for classification API token requests
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_APIKEY = "spark.marklogic.write.classifier.apikey";

    /**
     * Defines the number of documents and/or chunks of text to send to the classifier in a single request.
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_BATCH_SIZE = "spark.marklogic.write.classifier.batchSize";

    /**
     * Allows for passing any additional options to the text classifier.
     *
     * @since 2.6.0
     */
    public static final String WRITE_CLASSIFIER_OPTION_PREFIX = "spark.marklogic.write.classifier.option.";

    private Options() {
    }
}
