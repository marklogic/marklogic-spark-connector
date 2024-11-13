/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.client.document.DocumentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public interface Util {

    /**
     * Intended for all non-debug logging where the class name doesn't matter and only adds complexity to the log
     * messages.
     */
    Logger MAIN_LOGGER = LoggerFactory.getLogger("com.marklogic.spark");

    /**
     * Intended for log messages pertaining to the embedder feature. Uses a separate logger so that it can be enabled
     * at the info/debug level without enabling any other log messages.
     */
    Logger EMBEDDER_LOGGER = LoggerFactory.getLogger("com.marklogic.spark.embedder");

    static boolean hasOption(Map<String, String> properties, String... options) {
        return Stream.of(options)
            .anyMatch(option -> properties.get(option) != null && properties.get(option).trim().length() > 0);
    }

    /**
     * For parsing the Spark "paths" option, which is set when a user calls {@code load()} with 2 or more
     * paths. In that scenario, Spark effectively toString's the list of paths into a string of the form:
     * "["path", "path2", "path3"]", with the surrounding double quotes included.
     *
     * @param pathsValue
     * @return
     */
    static List<String> parsePaths(String pathsValue) {
        List<String> paths = new ArrayList<>();
        pathsValue = pathsValue.trim().substring(2, pathsValue.length() - 2);
        for (String path : pathsValue.split(",")) {
            if (path.charAt(0) == '"') {
                path = path.substring(1);
            }
            if (path.charAt(path.length() - 1) == '"') {
                path = path.substring(0, path.length() - 1);
            }
            paths.add(path);
        }
        return paths;
    }

    static boolean isReadWithCustomCodeOperation(Map<String, String> properties) {
        return Util.hasOption(properties,
            Options.READ_INVOKE, Options.READ_XQUERY, Options.READ_JAVASCRIPT,
            Options.READ_JAVASCRIPT_FILE, Options.READ_XQUERY_FILE
        );
    }

    static boolean isWriteWithCustomCodeOperation(Map<String, String> properties) {
        return Util.hasOption(properties,
            Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY,
            Options.WRITE_JAVASCRIPT_FILE, Options.WRITE_XQUERY_FILE
        );
    }

    /**
     * Allows Flux to override what's shown in a validation error. The connector is fine showing option names
     * such as "spark.marklogic.read.opticQuery", but that is meaningless to a Flux user. This can also be used to
     * access any key in the messages properties file.
     *
     * @param option
     * @return
     */
    static String getOptionNameForErrorMessage(String option) {
        ResourceBundle bundle = ResourceBundle.getBundle("marklogic-spark-messages", Locale.getDefault());
        String optionName = bundle.getString(option);
        return optionName != null && optionName.trim().length() > 0 ? optionName.trim() : option;
    }

    static Set<DocumentManager.Metadata> getRequestedMetadata(ContextSupport context) {
        Set<DocumentManager.Metadata> set = new HashSet<>();
        if (context.hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            for (String category : context.getStringOption(Options.READ_DOCUMENTS_CATEGORIES).split(",")) {
                if ("content".equalsIgnoreCase(category)) {
                    continue;
                }
                if ("metadata".equalsIgnoreCase(category)) {
                    set.add(DocumentManager.Metadata.ALL);
                } else {
                    set.add(DocumentManager.Metadata.valueOf(category.toUpperCase()));
                }
            }
        }
        return set;
    }
}
