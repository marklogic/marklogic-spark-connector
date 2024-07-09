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

import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.HashMap;

import java.util.*;
import java.util.stream.Stream;

public interface Util {

    /**
     * Intended for all non-debug logging where the class name doesn't matter and only adds complexity to the log
     * messages.
     */
    Logger MAIN_LOGGER = LoggerFactory.getLogger("com.marklogic.spark");

    JSONOptions DEFAULT_JSON_OPTIONS = new JSONOptions(
        new HashMap<>(),

        // As verified via tests, this default timezone ID is overridden by a user via the spark.sql.session.timeZone option.
        "Z",

        // We don't expect corrupted records - i.e. corrupted values - to be present in the index. But Spark
        // requires this to be set. See
        // https://medium.com/@sasidharan-r/how-to-handle-corrupt-or-bad-record-in-apache-spark-custom-logic-pyspark-aws-430ddec9bb41
        // for more information.
        "_corrupt_record"
    );

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
}
