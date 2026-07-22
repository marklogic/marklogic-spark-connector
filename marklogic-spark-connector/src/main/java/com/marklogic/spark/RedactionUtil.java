/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public abstract class RedactionUtil {

    static final String SPARK_REDACTION_REGEX = "spark.redaction.regex";

    static final String RECOMMENDED_SPARK_REDACTION_REGEX =
        "(?i).*password.*|.*apikey.*|.*connectionstring.*|.*secret.*|.*token.*";

    private static final List<String> SENSITIVE_OPTION_HINTS = Arrays.asList(
        "password", "apikey", "secret", "token", "connectionstring"
    );

    private static final Set<String> REDACTION_WARNING_KEYS = new HashSet<>();

    private RedactionUtil() {
    }

    static void warnIfSensitiveMarkLogicOptionsMayNotBeRedacted(Context context) {
        Set<String> likelySensitiveOptions = findLikelySensitiveMarkLogicOptions(context.getProperties());
        if (likelySensitiveOptions.isEmpty()) {
            return;
        }

        String redactionRegex = context.getStringOption(SPARK_REDACTION_REGEX);
        if (redactionRegex == null || redactionRegex.trim().isEmpty()) {
            try {
                redactionRegex = Util.getSparkSession().conf().get(SPARK_REDACTION_REGEX, null);
            } catch (Exception ignored) {
                // No active Spark session available, so only connector options can be inspected.
            }
        }

        Set<String> uncoveredOptions = findUnredactedSensitiveOptions(likelySensitiveOptions, redactionRegex);
        if (uncoveredOptions.isEmpty()) {
            return;
        }

        final String warningKey = uncoveredOptions.stream()
            .sorted()
            .collect(Collectors.joining(","));
        synchronized (REDACTION_WARNING_KEYS) {
            if (REDACTION_WARNING_KEYS.contains(warningKey)) {
                return;
            }
            REDACTION_WARNING_KEYS.add(warningKey);
        }

        Util.MAIN_LOGGER.warn(
            "Connector options {} appear likely to contain sensitive values but may not be redacted in Spark UI/event logs. " +
                "Set {}={} and enable Spark UI authentication/TLS for defense-in-depth.",
            uncoveredOptions,
            SPARK_REDACTION_REGEX,
            RECOMMENDED_SPARK_REDACTION_REGEX
        );
    }

    static Set<String> findLikelySensitiveMarkLogicOptions(Map<String, String> properties) {
        return properties.entrySet().stream()
            .filter(entry -> entry.getValue() != null && !entry.getValue().trim().isEmpty())
            .map(Map.Entry::getKey)
            .filter(RedactionUtil::isLikelySensitiveMarkLogicOption)
            .collect(Collectors.toSet());
    }

    static Set<String> findUnredactedSensitiveOptions(Set<String> likelySensitiveOptions, String redactionRegex) {
        if (redactionRegex == null || redactionRegex.trim().isEmpty()) {
            return new HashSet<>(likelySensitiveOptions);
        }

        try {
            Pattern redactionPattern = Pattern.compile(redactionRegex);
            return likelySensitiveOptions.stream()
                .filter(option -> !redactionPattern.matcher(option).find())
                .collect(Collectors.toSet());
        } catch (PatternSyntaxException ex) {
            return new HashSet<>(likelySensitiveOptions);
        }
    }

    static boolean isLikelySensitiveMarkLogicOption(String optionName) {
        if (optionName == null) {
            return false;
        }

        String lower = optionName.toLowerCase(Locale.ROOT);
        if (!lower.contains("marklogic")) {
            return false;
        }
        return SENSITIVE_OPTION_HINTS.stream().anyMatch(lower::contains);
    }
}
